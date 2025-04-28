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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.file.HudiLogFile;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.hudi.HudiUtil.constructSchema;

public class HudiSnapshotPageSource
        implements ConnectorPageSource
{
    private final HoodieStorage storage;
    private final String basePath;
    private final HudiSplit split;
    private final Optional<ConnectorPageSource> baseFilePageSource;
    private final Schema readerSchema;
    private final TypedProperties payloadProps = new TypedProperties();
    private final PageBuilder pageBuilder;
    private final HudiAvroSerializer avroSerializer;
    private final int recordKeyFieldPos;
    private final Map<Integer, String> partitionValueMap;

    private Map<String, HoodieRecord> logRecordMap;

    public HudiSnapshotPageSource(List<HivePartitionKey> partitionKeyList,
            HoodieStorage storage,
            String basePath,
            HudiSplit split,
            Optional<ConnectorPageSource> baseFilePageSource,
            List<HiveColumnHandle> dataHandles,
            List<HiveColumnHandle> columnHandles,
            Optional<String> preCombineField)
    {
        this.storage = storage;
        this.basePath = basePath;
        this.split = split;
        this.baseFilePageSource = baseFilePageSource;
        this.readerSchema = constructSchema(columnHandles.stream().map(HiveColumnHandle::getName).toList(),
                columnHandles.stream().map(HiveColumnHandle::getHiveType).toList(), false);
        this.pageBuilder = new PageBuilder(dataHandles.stream().map(HiveColumnHandle::getType).toList());
        Map<String, String> nameToPartitionValueMap = partitionKeyList.stream().collect(
                Collectors.toMap(e -> e.name(), e -> e.value()));
        this.partitionValueMap = new HashMap<>();
        for (int i = 0; i < dataHandles.size(); i++) {
            HiveColumnHandle handle = dataHandles.get(i);
            if (handle.isPartitionKey()) {
                partitionValueMap.put(i, nameToPartitionValueMap.get(handle.getName()));
            }
        }
        this.avroSerializer = new HudiAvroSerializer(columnHandles);
        this.recordKeyFieldPos = 2;
        preCombineField.ifPresent(s -> this.payloadProps.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, s));
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return (baseFilePageSource.isEmpty() || baseFilePageSource.get().isFinished())
                && (logRecordMap != null && logRecordMap.isEmpty());
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (baseFilePageSource.isPresent()) {
            return baseFilePageSource.get().isBlocked();
        }
        return CompletableFuture.completedFuture(0);
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        if (baseFilePageSource.isPresent()) {
            return baseFilePageSource.get().getCompletedPositions();
        }
        return OptionalLong.of(10);
    }

    @Override
    public Metrics getMetrics()
    {
        if (baseFilePageSource.isPresent()) {
            return baseFilePageSource.get().getMetrics();
        }
        return Metrics.EMPTY;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (logRecordMap == null) {
            try (HoodieMergedLogRecordScanner logScanner = getMergedLogRecordScanner(storage, basePath, split, readerSchema)) {
                logRecordMap = logScanner.getRecords();
            }
            catch (IOException e) {
                throw new HoodieIOException("Cannot read Hudi split " + split, e);
            }
        }

        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");

        if (baseFilePageSource.isPresent()) {
            SourcePage sourcePage = baseFilePageSource.get().getNextSourcePage();
            if (sourcePage != null) {
                try {
                    // Merge records from the page with log records
                    for (int pos = 0; pos < sourcePage.getPositionCount(); pos++) {
                        String recordKey = (String) avroSerializer.getValue(sourcePage, recordKeyFieldPos, pos);
                        HoodieRecord logRecord = logRecordMap.remove(recordKey);
                        if (logRecord != null) {
                            // Merging base and log
                            IndexedRecord baseRecord = avroSerializer.serialize(sourcePage, pos);
                            Option<HoodieAvroIndexedRecord> mergedRecord = mergeRecord(baseRecord, logRecord);
                            if (mergedRecord.isEmpty()) {
                                continue;
                            }
                            avroSerializer.buildRecordInPage(pageBuilder, mergedRecord.get().getData(), partitionValueMap, true);
                        }
                        else {
                            avroSerializer.buildRecordInPage(pageBuilder, sourcePage, pos, partitionValueMap, false);
                        }
                    }

                    Page newPage = pageBuilder.build();
                    pageBuilder.reset();
                    return SourcePage.create(newPage);
                }
                catch (IOException e) {
                    throw new HoodieIOException("Cannot merge record in split " + split);
                }
            }
        }

        if (logRecordMap.isEmpty()) {
            return null;
        }

        // Sending the rest to a page
        for (HoodieRecord hudiRecord : logRecordMap.values()) {
            IndexedRecord record = ((HoodieAvroIndexedRecord) hudiRecord).getData();
            avroSerializer.buildRecordInPage(pageBuilder, record, partitionValueMap, true);
        }

        logRecordMap.clear();
        Page newPage = pageBuilder.build();
        pageBuilder.reset();
        return SourcePage.create(newPage);
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }

    private static HoodieMergedLogRecordScanner getMergedLogRecordScanner(HoodieStorage storage,
            String basePath,
            HudiSplit split,
            Schema readerSchema)
            throws IOException
    {
        return HoodieMergedLogRecordScanner.newBuilder()
                .withStorage(storage)
                .withBasePath(basePath)
                .withLogFilePaths(split.getLogFiles().stream().map(HudiLogFile::getPath).collect(Collectors.toList()))
                .withReaderSchema(readerSchema)
                .withLatestInstantTime(split.getCommitTime())
                .withMaxMemorySizeInBytes(1 * 1024 * 1024L)
                .withReverseReader(false)
                .withBufferSize(1024 * 1024)
                .withSpillableMapBasePath("/tmp/")
                .withDiskMapType(ExternalSpillableMap.DiskMapType.BITCASK)
                .withBitCaskDiskMapCompressionEnabled(true)
                .withOptimizedLogBlocksScan(false)
                .withInternalSchema(InternalSchema.getEmptyInternalSchema())
                .build();
    }

    private Option<HoodieAvroIndexedRecord> mergeRecord(IndexedRecord baseRecord, HoodieRecord<?> newRecord)
            throws IOException
    {
        HoodieAvroIndexedRecord baseHudiRecord = new HoodieAvroIndexedRecord(baseRecord);
        Option<Pair<HoodieRecord, Schema>> mergeResult = HoodieAvroRecordMerger.INSTANCE.merge(
                baseHudiRecord, baseRecord.getSchema(), newRecord, readerSchema, payloadProps);
        return mergeResult.map(p -> (HoodieAvroIndexedRecord) p.getLeft());
    }
}
