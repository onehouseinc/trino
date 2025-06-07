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
package io.trino.plugin.hudi.query;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.metastore.Partition;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class HudiSnapshotDirectoryLister
        implements HudiDirectoryLister
{
    private static final Logger log = Logger.get(HudiSnapshotDirectoryLister.class);
    private final Lazy<String> commitTime;
    private final Lazy<HoodieTableFileSystemView> lazyFileSystemView;
    private final Lazy<Map<String, HudiPartitionInfo>> lazyAllPartitionInfoMap;

    public HudiSnapshotDirectoryLister(
            HudiTableHandle tableHandle,
            boolean enableMetadataTable,
            List<HiveColumnHandle> partitionColumnHandles,
            Lazy<Map<String, Partition>> lazyAllPartitions,
            Lazy<String> commitTime)
    {
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(enableMetadataTable)
                .build();
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        this.commitTime = commitTime;
        this.lazyFileSystemView = Lazy.lazily(() -> {
            HoodieTimer timer = HoodieTimer.start();
            HoodieTableFileSystemView fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
                    new HoodieLocalEngineContext(new TrinoStorageConfiguration()), tableHandle.getMetaClient(), metadataConfig);
            if (enableMetadataTable) {
                fileSystemView.loadAllPartitions();
            }
            log.info("Created file system view of table %s in %s ms", schemaTableName, timer.endTimer());
            return fileSystemView;
        });
        this.lazyAllPartitionInfoMap = Lazy.lazily(() -> lazyAllPartitions.get().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new HiveHudiPartitionInfo(
                                schemaTableName,
                                Location.of(tableHandle.getBasePath()),
                                e.getKey(),
                                e.getValue(),
                                partitionColumnHandles,
                                tableHandle.getPartitionPredicates()))));
    }

    @Override
    public List<FileSlice> listStatus(HudiPartitionInfo partitionInfo)
    {
        ImmutableList<FileSlice> collect = lazyFileSystemView.get()
                .getLatestFileSlicesBeforeOrOn(partitionInfo.getRelativePartitionPath(), commitTime.get(), false)
                .collect(toImmutableList());
        return collect;
    }

    @Override
    public Optional<HudiPartitionInfo> getPartitionInfo(String partition)
    {
        return Optional.ofNullable(lazyAllPartitionInfoMap.get().get(partition));
    }

    @Override
    public void close()
    {
        if (!lazyFileSystemView.get().isClosed()) {
            lazyFileSystemView.get().close();
        }
    }
}
