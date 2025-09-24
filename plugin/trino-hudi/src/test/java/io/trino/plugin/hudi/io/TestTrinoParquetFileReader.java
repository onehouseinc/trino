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

import com.google.common.io.Resources;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.parquet.Column;
import io.trino.plugin.hudi.storage.HudiTrinoStorage;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTrinoParquetFileReader
{
    private static final String ARCHIVE_TIMELINE_PARQUET_FILE = "archived_timeline.parquet";

    @Test
    public void testReadParquetFile()
            throws Exception
    {
        // Use the existing test Parquet file (Should have only 2 rows)
        File parquetFile = new File(Resources.getResource(ARCHIVE_TIMELINE_PARQUET_FILE).toURI());
        assertThat(parquetFile.exists()).isTrue();
        assertThat(parquetFile.length()).isGreaterThan(0);

        // Create HoodieStorage and StoragePath
        HoodieStorage storage = new HudiTrinoStorage(new LocalFileSystem(Paths.get("/")), new TrinoStorageConfiguration());
        StoragePath storagePath = new StoragePath(parquetFile.toURI().toString());

        try (TrinoParquetFileReader reader = new TrinoParquetFileReader(storage, storagePath)) {
            // Verify we can get the schema
            Schema schema = reader.getSchema();
            assertThat(schema).isNotNull();
            assertThat(schema.getFields()).isNotEmpty();

            // Verify we can read records
            ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(schema, null);

            // Collect all records
            List<IndexedRecord> records = new ArrayList<>();
            iterator.forEachRemaining(records::add);
            iterator.close();

            // Verify we read at least some records and the total count matches
            assertThat(records).isNotEmpty();
            long totalRecords = reader.getTotalRecords();
            assertThat(totalRecords).isEqualTo(records.size());
            // Ensure we have enough records to test
            assertThat(records.size()).isGreaterThanOrEqualTo(2);

            // Verify the content of the first two records
            IndexedRecord record1 = records.getFirst();
            assertThat(record1.get(schema.getField("instantTime").pos()).toString()).isEqualTo("20250918121953134");
            assertThat(record1.get(schema.getField("completionTime").pos()).toString()).isEqualTo("20250918121957816");
            assertThat(record1.get(schema.getField("action").pos()).toString()).isEqualTo("commit");

            IndexedRecord record2 = records.get(1);
            assertThat(record2.get(schema.getField("instantTime").pos()).toString()).isEqualTo("20250918121958100");
            assertThat(record2.get(schema.getField("completionTime").pos()).toString()).isEqualTo("20250918121959081");
            assertThat(record2.get(schema.getField("action").pos()).toString()).isEqualTo("commit");
        }
    }

    @Test
    public void testReadEmptySchema()
            throws Exception
    {
        // Test with empty requested schema to verify schema handling
        File parquetFile = new File(Resources.getResource(ARCHIVE_TIMELINE_PARQUET_FILE).toURI());
        HoodieStorage storage = new HudiTrinoStorage(new LocalFileSystem(Paths.get("/")), new TrinoStorageConfiguration());
        StoragePath storagePath = new StoragePath(parquetFile.toURI().toString());

        try (TrinoParquetFileReader reader = new TrinoParquetFileReader(storage, storagePath)) {
            Schema originalSchema = reader.getSchema();

            // Test with null requested schema (should use avro schema from footer)
            ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(originalSchema, null);

            if (iterator.hasNext()) {
                IndexedRecord record = iterator.next();
                assertThat(record.getSchema()).isEqualTo(originalSchema);
            }

            iterator.close();
        }
    }

    @Test
    public void testReadBloomFilter()
            throws Exception
    {
        // Test reading bloom filter from Parquet metadata
        // For archived timeline parquet files, no bloom filter in footer, hence null
        File parquetFile = new File(Resources.getResource(ARCHIVE_TIMELINE_PARQUET_FILE).toURI());
        HoodieStorage storage = new HudiTrinoStorage(new LocalFileSystem(Paths.get("/")), new TrinoStorageConfiguration());
        StoragePath storagePath = new StoragePath(parquetFile.toURI().toString());

        try (TrinoParquetFileReader reader = new TrinoParquetFileReader(storage, storagePath)) {
            BloomFilter bloomFilter = reader.readBloomFilter();
            // The important test is that the method doesn't throw exceptions and handles missing bloom filter metadata gracefully
            assertThat(bloomFilter).isNull();
        }
    }

    @Test
    public void testReadMinMaxRecordKeys()
            throws Exception
    {
        // Test reading min/max record keys from Parquet metadata
        // For archived timeline parquet files, no min and max record keys in footer
        File parquetFile = new File(Resources.getResource(ARCHIVE_TIMELINE_PARQUET_FILE).toURI());
        HoodieStorage storage = new HudiTrinoStorage(new LocalFileSystem(Paths.get("/")), new TrinoStorageConfiguration());
        StoragePath storagePath = new StoragePath(parquetFile.toURI().toString());

        try (TrinoParquetFileReader reader = new TrinoParquetFileReader(storage, storagePath)) {
            String[] minMaxKeys = reader.readMinMaxRecordKeys();

            // Method should always return a non-null array of size 0
            assertThat(minMaxKeys).isNotNull();
            assertThat(minMaxKeys.length).isEqualTo(0);
        }
    }

    @Test
    public void testBuildTrinoColumns()
    {
        // Direct unit test for buildTrinoColumns static method
        // Create a simple Avro schema with multiple fields
        Schema avroSchema = Schema.createRecord("testRecord", null, null, false);
        avroSchema.setFields(List.of(
                new Schema.Field("field1", Schema.create(Schema.Type.STRING)),
                new Schema.Field("field2", Schema.create(Schema.Type.LONG)),
                new Schema.Field("field3", Schema.create(Schema.Type.BOOLEAN))));

        // Create a matching Parquet schema
        MessageType parquetSchema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).named("field1")
                .required(PrimitiveType.PrimitiveTypeName.INT64).named("field2")
                .required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named("field3")
                .named("testMessage");

        // Test the buildTrinoColumns method directly
        List<Column> columns = TrinoParquetFileReader.buildTrinoColumns(avroSchema, parquetSchema);

        // Verify we got the expected number of columns
        assertThat(columns).hasSize(3);

        // Each column should have the correct individual name
        assertThat(columns.get(0).name()).isEqualTo("field1");
        assertThat(columns.get(1).name()).isEqualTo("field2");
        assertThat(columns.get(2).name()).isEqualTo("field3");

        // Verify all names are different
        assertThat(columns.get(0).name()).isNotEqualTo(columns.get(1).name());
        assertThat(columns.get(0).name()).isNotEqualTo(columns.get(2).name());
        assertThat(columns.get(1).name()).isNotEqualTo(columns.get(2).name());
    }

    private boolean isDefaultValue(Object value, Schema schema)
    {
        // Handle union types (nullable fields)
        if (schema.getType() == Schema.Type.UNION) {
            for (Schema unionSchema : schema.getTypes()) {
                if (unionSchema.getType() != Schema.Type.NULL) {
                    schema = unionSchema;
                    break;
                }
            }
        }

        return switch (schema.getType()) {
            case STRING -> "default_value".equals(value);
            case INT -> Integer.valueOf(0).equals(value);
            case LONG -> Long.valueOf(0L).equals(value);
            case FLOAT -> Float.valueOf(0.0f).equals(value);
            case DOUBLE -> Double.valueOf(0.0d).equals(value);
            case BOOLEAN -> Boolean.FALSE.equals(value);
            case BYTES -> value instanceof byte[] && ((byte[]) value).length == 0;
            default -> false;
        };
    }
}
