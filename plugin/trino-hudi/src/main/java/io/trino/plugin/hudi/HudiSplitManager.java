/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Partition;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_PARTITION_NOT_FOUND;
import static io.trino.plugin.hudi.HudiSessionProperties.getDynamicFilteringWaitTimeout;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxOutstandingSplits;
import static io.trino.plugin.hudi.HudiSessionProperties.getMaxSplitsPerSecond;
import static io.trino.plugin.hudi.partition.HiveHudiPartitionInfo.NON_PARTITION;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private final TypeManager typeManager;
    private final BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final ExecutorService executor;
    private final ScheduledExecutorService splitLoaderExecutorService;

    @Inject
    public HudiSplitManager(
            TypeManager typeManager,
            BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> metastoreProvider,
            @ForHudiSplitManager ExecutorService executor,
            TrinoFileSystemFactory fileSystemFactory,
            @ForHudiSplitSource ScheduledExecutorService splitLoaderExecutorService)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metastoreProvider = requireNonNull(metastoreProvider, "metastoreProvider is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.splitLoaderExecutorService = requireNonNull(splitLoaderExecutorService, "splitLoaderExecutorService is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        HudiTableHandle hudiTableHandle = (HudiTableHandle) tableHandle;
        HiveMetastore metastore = metastoreProvider.apply(session.getIdentity(), (HiveTransactionHandle) transaction);
        Table table = metastore.getTable(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(schemaTableName(hudiTableHandle.getSchemaName(), hudiTableHandle.getTableName())));

        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(table, typeManager);
        Map<String, HiveColumnHandle> partitionColumnHandles = partitionColumns.stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
        Map<String, Partition> allPartitions = getPartitions(metastore, hudiTableHandle, table, partitionColumns);

        HudiSplitSource splitSource = new HudiSplitSource(
                session,
                table,
                hudiTableHandle,
                fileSystemFactory,
                partitionColumnHandles,
                executor,
                splitLoaderExecutorService,
                getMaxSplitsPerSecond(session),
                getMaxOutstandingSplits(session),
                allPartitions,
                dynamicFilter,
                getDynamicFilteringWaitTimeout(session));
        return new ClassLoaderSafeConnectorSplitSource(splitSource, HudiSplitManager.class.getClassLoader());
    }

    private static Map<String, Partition> getPartitions(
            HiveMetastore metastore,
            HudiTableHandle tableHandle,
            Table table,
            List<HiveColumnHandle> partitionColumns)
    {
        if (partitionColumns.isEmpty()) {
            return ImmutableMap.of(
                    NON_PARTITION, Partition.builder()
                            .setDatabaseName(tableHandle.getSchemaName())
                            .setTableName(tableHandle.getTableName())
                            .withStorage(storageBuilder ->
                                    storageBuilder.setLocation(tableHandle.getBasePath())
                                            .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                            .setColumns(ImmutableList.of())
                            .setValues(ImmutableList.of())
                            .build());
        }

        List<String> partitionNames = metastore.getPartitionNamesByFilter(
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        partitionColumns.stream().map(HiveColumnHandle::getName).collect(Collectors.toList()),
                        computePartitionKeyFilter(partitionColumns, tableHandle.getPartitionPredicates()))
                .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
        Map<String, Optional<Partition>> partitionsByNames = metastore.getPartitionsByNames(table, partitionNames);
        List<String> partitionsNotFound = partitionsByNames.entrySet().stream().filter(e -> e.getValue().isEmpty()).map(Map.Entry::getKey).toList();
        if (!partitionsNotFound.isEmpty()) {
            throw new TrinoException(HUDI_PARTITION_NOT_FOUND, format("Cannot find partitions in metastore: %s", partitionsNotFound));
        }
        return partitionsByNames
                .entrySet().stream()
                .filter(e -> e.getValue().isPresent())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
    }
}
