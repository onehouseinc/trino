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
package io.trino.plugin.hudi.io;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.metastore.HiveType;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetCorruptionException;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.BlockMetadata;
import io.trino.parquet.metadata.FileMetadata;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.HoodieBloomFilterWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.parquet.ParquetTypeUtils.constructField;
import static io.trino.parquet.ParquetTypeUtils.getColumnIO;
import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.trino.parquet.metadata.PrunedBlockMetadata.createPrunedColumnsMetadata;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createDataSource;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_SCHEMA_ERROR;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.avro.HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;

public class TrinoParquetFileReader
        extends HoodieAvroFileReader
{
    private static final String PARQUET_AVRO_SCHEMA_KEY = "parquet.avro.schema";
    private static final DateTimeZone UTC_TIME_ZONE = DateTimeZone.UTC;

    private final HudiTrinoStorage trinoStorage;
    private final StoragePath path;
    private final ParquetMetadata parquetMetadata;
    private final Schema avroSchema;
    private final long totalRecords;
    private final ParquetReaderOptions readerOptions = new ParquetReaderOptions();

    public TrinoParquetFileReader(HoodieStorage storage, StoragePath path)
            throws IOException
    {
        this.path = requireNonNull(path, "path is null");
        requireNonNull(storage, "storage is null");
        checkArgument(storage instanceof HudiTrinoStorage, "storage must be an instance of HudiTrinoStorage");
        this.trinoStorage = (HudiTrinoStorage) storage;

        this.parquetMetadata = readParquetMetadata();
        this.avroSchema = extractAvroSchema(parquetMetadata.getFileMetaData());
        this.totalRecords = calculateTotalRecords(parquetMetadata);
    }

    private ParquetMetadata readParquetMetadata()
            throws IOException
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        try (ParquetDataSource dataSource = createDataSourceInternal(memoryContext)) {
            return MetadataReader.readFooter(dataSource, Optional.empty());
        }
    }

    private Schema extractAvroSchema(FileMetadata fileMetaData)
    {
        String avroSchemaStr = fileMetaData.getKeyValueMetaData().get(PARQUET_AVRO_SCHEMA_KEY);
        if (avroSchemaStr == null) {
            throw new TrinoException(HUDI_SCHEMA_ERROR, "Parquet file does not contain Avro schema in metadata: " + path);
        }
        return new Schema.Parser().parse(avroSchemaStr);
    }

    private long calculateTotalRecords(ParquetMetadata metadata)
            throws ParquetCorruptionException
    {
        return metadata.getBlocks().stream()
                .mapToLong(BlockMetadata::rowCount)
                .sum();
    }

    private ParquetDataSource createDataSourceInternal(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        TrinoFileSystem fileSystem = (TrinoFileSystem) trinoStorage.getFileSystem();
        TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(path.toString()));
        FileFormatDataSourceStats stats = new FileFormatDataSourceStats();
        return createDataSource(inputFile, OptionalLong.empty(), readerOptions, memoryContext, stats);
    }

    @Override
    public ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema, Schema requestedSchema)
            throws IOException
    {
        Schema schema = requestedSchema != null ? requestedSchema : this.avroSchema;
        return new ParquetIndexedRecordIterator(schema);
    }

    /**
     * Adapted from org.apache.hudi.common.util.FileFormatUtils#readMinMaxRecordKeys
     */
    @Override
    public String[] readMinMaxRecordKeys()
    {
        Map<String, String> keyValueMetaData = parquetMetadata.getFileMetaData().getKeyValueMetaData();
        String minKey = keyValueMetaData.get(HOODIE_MIN_RECORD_KEY_FOOTER);
        String maxKey = keyValueMetaData.get(HOODIE_MAX_RECORD_KEY_FOOTER);

        if (minKey != null && maxKey != null) {
            return new String[] {minKey, maxKey};
        }
        return new String[0];
    }

    /**
     * Adapted from org.apache.hudi.common.util.FileFormatUtils#readBloomFilterFromMetadata
     */
    @Override
    public BloomFilter readBloomFilter()
    {
        Map<String, String> keyValueMetaData = parquetMetadata.getFileMetaData().getKeyValueMetaData();
        String footerVal = keyValueMetaData.get(HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
        if (null == footerVal) {
            // We use old style key "com.uber.hoodie.bloomfilter"
            footerVal = keyValueMetaData.get(HoodieBloomFilterWriteSupport.OLD_HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
        }
        BloomFilter toReturn = null;
        if (footerVal != null) {
            if (keyValueMetaData.containsKey(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE)) {
                toReturn = BloomFilterFactory.fromString(footerVal,
                        keyValueMetaData.get(HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE));
            }
            else {
                toReturn = BloomFilterFactory.fromString(footerVal, BloomFilterTypeCode.SIMPLE.name());
            }
        }
        return toReturn;
    }

    @Override
    public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys)
    {
        throw new UnsupportedOperationException("Filtering row keys is not supported by this reader");
    }

    @Override
    public ClosableIterator<String> getRecordKeyIterator()
    {
        throw new UnsupportedOperationException("Iterating over only record keys is not supported by this reader");
    }

    @Override
    public Schema getSchema()
    {
        return avroSchema;
    }

    @Override
    public void close()
    {
        // No-op. Resources are managed within the iterator or during initialization
    }

    @Override
    public long getTotalRecords()
    {
        return totalRecords;
    }

    static List<Column> buildTrinoColumns(Schema readerSchema, MessageType fileParquetSchema)
    {
        MessageColumnIO messageColumnIO = getColumnIO(fileParquetSchema, fileParquetSchema);
        ImmutableList.Builder<Column> columnsBuilder = ImmutableList.builder();

        for (Schema.Field field : readerSchema.getFields()) {
            Type trinoType = avroTypeToTrinoType(field.schema());
            Field parquetField = constructField(trinoType, lookupColumnByName(messageColumnIO, field.name()))
                    .orElseThrow(() -> new TrinoException(HUDI_SCHEMA_ERROR, "Could not find column: " + field.name()));
            columnsBuilder.add(new Column(field.name(), parquetField));
        }
        return columnsBuilder.build();
    }

    private static Type avroTypeToTrinoType(Schema fieldSchema)
    {
        // Handle Avro's nullable fields, which are represented as a UNION of null and a type
        if (fieldSchema.isUnion()) {
            Optional<Schema> nonNullSchema = fieldSchema.getTypes().stream()
                    .filter(s -> s.getType() != Schema.Type.NULL)
                    .findFirst();
            // If it's a union of multiple non-null types, we do not support it
            if (nonNullSchema.isEmpty() || fieldSchema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).count() > 1) {
                throw new UnsupportedOperationException("Unsupported Avro union type: " + fieldSchema);
            }
            fieldSchema = nonNullSchema.get();
        }

        return switch (fieldSchema.getType()) {
            case STRING -> VarcharType.VARCHAR;
            case INT -> IntegerType.INTEGER;
            case LONG -> BigintType.BIGINT;
            case FLOAT -> RealType.REAL;
            case DOUBLE -> DoubleType.DOUBLE;
            case BOOLEAN -> BooleanType.BOOLEAN;
            case BYTES -> VarbinaryType.VARBINARY;
            // Be explicit about unhandled types instead of a silent fallback to prevent subtle bugs if the schema contains
            // types like MAP, ARRAY, FIXED, etc
            default -> throw new UnsupportedOperationException("Unsupported Avro type: " + fieldSchema.getType());
        };
    }

    private class ParquetIndexedRecordIterator
            implements ClosableIterator<IndexedRecord>
    {
        private final ParquetReader parquetReader;
        private final HudiAvroSerializer avroSerializer;
        private Page currentPage;
        private int currentPosition;
        private boolean closed;

        public ParquetIndexedRecordIterator(Schema readerSchema)
                throws IOException
        {
            // The outer class's fields are now final and can be safely accessed
            this.parquetReader = createParquetReader(readerSchema);
            this.avroSerializer = createAvroSerializer(readerSchema);
            // Pre-load the first page
            loadNextPage();
        }

        private ParquetReader createParquetReader(Schema readerSchema)
                throws IOException
        {
            AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
            ParquetDataSource dataSource = createDataSourceInternal(memoryContext);

            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            List<Column> columns = buildTrinoColumns(readerSchema, fileMetaData.getSchema());
            List<RowGroupInfo> rowGroups = buildRowGroupInfos(dataSource.getId());

            return new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    columns,
                    rowGroups,
                    dataSource,
                    UTC_TIME_ZONE,
                    memoryContext,
                    readerOptions,
                    exception -> new TrinoException(HUDI_SCHEMA_ERROR, "Failed to read Parquet file: " + path, exception),
                    Optional.empty(),
                    Optional.empty());
        }

        private List<RowGroupInfo> buildRowGroupInfos(ParquetDataSourceId dataSourceId)
                throws ParquetCorruptionException
        {
            FileMetadata fileMetaData = parquetMetadata.getFileMetaData();
            Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileMetaData.getSchema(), fileMetaData.getSchema());
            List<RowGroupInfo> rowGroups = new ArrayList<>();
            long startOffset = 0;
            for (BlockMetadata block : parquetMetadata.getBlocks()) {
                rowGroups.add(new RowGroupInfo(createPrunedColumnsMetadata(block, dataSourceId, descriptorsByPath), startOffset, Optional.empty()));
                startOffset += block.rowCount();
            }
            return rowGroups;
        }

        private void loadNextPage()
                throws IOException
        {
            currentPage = parquetReader.nextPage();
            currentPosition = 0;
        }

        private HudiAvroSerializer createAvroSerializer(Schema readerSchema)
        {
            List<HiveColumnHandle> columnHandles = new ArrayList<>();
            List<Schema.Field> fields = readerSchema.getFields();

            for (int i = 0; i < fields.size(); i++) {
                Schema.Field field = fields.get(i);
                Type trinoType = avroTypeToTrinoType(field.schema());
                HiveType hiveType = toHiveType(trinoType);

                columnHandles.add(new HiveColumnHandle(
                        field.name(),
                        i,
                        hiveType,
                        trinoType,
                        Optional.empty(),
                        HiveColumnHandle.ColumnType.REGULAR,
                        Optional.empty()));
            }

            return new HudiAvroSerializer(columnHandles, readerSchema, Optional.empty());
        }

        private static Type avroTypeToTrinoType(Schema fieldSchema)
        {
            // Handle Avro's nullable fields, which are represented as a UNION of null and a type
            if (fieldSchema.isUnion()) {
                Optional<Schema> nonNullSchema = fieldSchema.getTypes().stream()
                        .filter(s -> s.getType() != Schema.Type.NULL)
                        .findFirst();
                // If it's a union of multiple non-null types, we do not support it
                if (nonNullSchema.isEmpty() || fieldSchema.getTypes().stream().filter(s -> s.getType() != Schema.Type.NULL).count() > 1) {
                    throw new UnsupportedOperationException("Unsupported Avro union type: " + fieldSchema);
                }
                fieldSchema = nonNullSchema.get();
            }

            return switch (fieldSchema.getType()) {
                case STRING -> VarcharType.VARCHAR;
                case INT -> IntegerType.INTEGER;
                case LONG -> BigintType.BIGINT;
                case FLOAT -> RealType.REAL;
                case DOUBLE -> DoubleType.DOUBLE;
                case BOOLEAN -> BooleanType.BOOLEAN;
                case BYTES -> VarbinaryType.VARBINARY;
                // Be explicit about unhandled types instead of a silent fallback to prevent subtle bugs if the schema contains
                // types like MAP, ARRAY, FIXED, etc
                default -> throw new UnsupportedOperationException("Unsupported Avro type: " + fieldSchema.getType());
            };
        }

        @Override
        public boolean hasNext()
        {
            if (closed) {
                return false;
            }

            if (currentPage != null && currentPosition < currentPage.getPositionCount()) {
                return true;
            }

            try {
                loadNextPage();
                return currentPage != null && currentPage.getPositionCount() > 0;
            }
            catch (IOException e) {
                throw new TrinoException(HUDI_SCHEMA_ERROR, "Failed to load next page from Parquet file", e);
            }
        }

        @Override
        public IndexedRecord next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            IndexedRecord record = avroSerializer.serialize(currentPage, currentPosition);
            currentPosition++;
            return record;
        }

        @Override
        public void close()
        {
            if (!closed) {
                closed = true;
                try {
                    parquetReader.close();
                }
                catch (IOException e) {
                    throw new RuntimeException("Failed to close ParquetReader", e);
                }
            }
        }
    }
}
