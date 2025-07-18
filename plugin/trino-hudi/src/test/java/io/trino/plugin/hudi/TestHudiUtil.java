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
import io.trino.metastore.Column;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.StorageFormat;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hudi.HudiUtil.buildPartition;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHudiUtil
{
    @Test
    public void testBuildHiveHudiPartitionInfo()
    {
        // Setup mock HudiTableHandle
        String partitionColumnName1 = "year";
        String partitionColumnName2 = "month";

        List<HiveColumnHandle> partitionColumns = ImmutableList.of(
                new HiveColumnHandle(partitionColumnName1, 0, HIVE_STRING, VARCHAR, Optional.empty(), PARTITION_KEY, Optional.empty()),
                new HiveColumnHandle(partitionColumnName2, 0, HIVE_STRING, VARCHAR, Optional.empty(), PARTITION_KEY, Optional.empty()));

        HudiTableHandle tableHandle = new HudiTableHandle(
                "test_schema",
                "test_table",
                "s3://bucket/test_path",
                HoodieTableType.MERGE_ON_READ,
                partitionColumns,
                TupleDomain.all(),
                TupleDomain.all());

        // Setup dummy partition
        Partition partition = Partition.builder()
                .setDatabaseName("test_schema")
                .setTableName("test_table")
                .setValues(List.of("2024", "07"))
                .setColumns(List.of(
                        new Column("year", HiveType.HIVE_STRING, Optional.empty(), Map.of()),
                        new Column("month", HiveType.HIVE_STRING, Optional.empty(), Map.of())))
                .withStorage(storageBuilder ->
                        storageBuilder.setLocation("s3://bucket/test_path/year=2024/month=07")
                                .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                .build();

        String partitionName = "year=2024/month=07";

        // Call the method
        HiveHudiPartitionInfo result = HudiUtil.buildHiveHudiPartitionInfo(
                tableHandle, partitionName, partition);

        // Assertions
        assertThat(result.getRelativePartitionPath()).isEqualTo("year=2024/month=07");
        assertThat(result.getHivePartitionKeys()).containsExactly(
                new HivePartitionKey(partitionColumnName1, "2024"),
                new HivePartitionKey(partitionColumnName2, "07"));
        assertThat(result.getHivePartitionName()).isEqualTo(partitionName);
    }

    @Test
    public void testBuildPartition_withValidPartitionPath()
    {
        String partitionColumn1 = "year";
        String partitionColumn2 = "month";

        List<HiveColumnHandle> partitionColumns = List.of(
                new HiveColumnHandle(partitionColumn1, 0, HIVE_STRING, VARCHAR, Optional.empty(), PARTITION_KEY, Optional.empty()),
                new HiveColumnHandle(partitionColumn2, 1, HIVE_STRING, VARCHAR, Optional.empty(), PARTITION_KEY, Optional.empty()));

        HudiTableHandle tableHandle = new HudiTableHandle(
                "test_schema",
                "test_table",
                "s3://bucket/test_path",
                HoodieTableType.MERGE_ON_READ,
                partitionColumns,
                TupleDomain.all(),
                TupleDomain.all());

        String partitionPath = "year=2024/month=07";

        // Simple extractor based on hive-style (split by '=' and '/')
        PartitionValueExtractor extractor = new MultiPartKeysValueExtractor();

        Partition partition = buildPartition(partitionPath, tableHandle, extractor);

        assertThat(partition.getDatabaseName()).isEqualTo("test_schema");
        assertThat(partition.getTableName()).isEqualTo("test_table");
        assertThat(partition.getValues()).isEqualTo(List.of("2024", "07"));
        assertThat(partition.getStorage().getLocation()).isEqualTo("s3://bucket/test_path/year=2024/month=07");
        assertThat(partition.getColumns().size()).isEqualTo(2);
        assertThat(partition.getColumns().get(0).getName()).isEqualTo("year");
        assertThat(partition.getColumns().get(1).getName()).isEqualTo("month");
    }

    @Test
    public void testBuildPartition_withEmptyPartitionPath()
    {
        HudiTableHandle tableHandle = new HudiTableHandle(
                "test_schema",
                "test_table",
                "s3://bucket/test_path",
                HoodieTableType.COPY_ON_WRITE,
                List.of(), // no partition columns
                TupleDomain.all(),
                TupleDomain.all());

        PartitionValueExtractor extractor = path -> List.of();

        Partition partition = buildPartition("", tableHandle, extractor);

        assertThat(partition.getDatabaseName()).isEqualTo("test_schema");
        assertThat(partition.getTableName()).isEqualTo("test_table");
        assertThat(partition.getValues()).isEqualTo(List.of());
        assertThat(partition.getColumns()).isEqualTo(List.of());
        assertThat(partition.getStorage().getLocation()).isEqualTo("s3://bucket/test_path");
    }

    @Test
    public void testBuildPartition_withMismatchedPartitionValues_throws()
    {
        List<HiveColumnHandle> partitionColumns = List.of(
                new HiveColumnHandle("year", 0, HIVE_STRING, VARCHAR, Optional.empty(), PARTITION_KEY, Optional.empty()));

        HudiTableHandle tableHandle = new HudiTableHandle(
                "test_schema",
                "test_table",
                "s3://bucket/test_path",
                HoodieTableType.COPY_ON_WRITE,
                partitionColumns,
                TupleDomain.all(),
                TupleDomain.all());

        String invalidPartitionPath = "year=2024/month=07"; // 2 values, 1 column

        PartitionValueExtractor extractor = path -> Arrays.stream(path.split("/"))
                .map(p -> p.split("=")[1])
                .toList();

        assertThatThrownBy(() -> buildPartition(invalidPartitionPath, tableHandle, extractor))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Invalid partition path");
    }

    @Test
    public void testGetHivePartitionName_validPartition()
    {
        Partition partition = Partition.builder()
                .setDatabaseName("test_schema")
                .setTableName("test_table")
                .setColumns(List.of(
                        new Column("year", HiveType.HIVE_STRING, Optional.empty(), Map.of()),
                        new Column("month", HiveType.HIVE_STRING, Optional.empty(), Map.of())))
                .setValues(List.of("2024", "07"))
                .withStorage(storageBuilder ->
                        storageBuilder.setLocation("s3://bucket/test_path/year=2024/month=07")
                                .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                .build();

        String result = HudiUtil.getHivePartitionName(partition);
        assertThat(result).isEqualTo("year=2024/month=07");
    }

    @Test
    public void testGetHivePartitionName_emptyPartition()
    {
        Partition partition = Partition.builder()
                .setDatabaseName("test_schema")
                .setTableName("test_table")
                .setColumns(List.of())
                .setValues(List.of())
                .withStorage(storageBuilder ->
                        storageBuilder.setLocation("s3://bucket/test_path")
                                .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                .build();

        String result = HudiUtil.getHivePartitionName(partition);
        assertThat(result).isEmpty();
    }

    @Test
    public void testGetHivePartitionName_mismatchedColumnsAndValues_throws()
    {
        Partition partition = Partition.builder()
                .setDatabaseName("test_schema")
                .setTableName("test_table")
                .setColumns(List.of(
                        new Column("year", HiveType.HIVE_STRING, Optional.empty(), Map.of())))
                .setValues(List.of("2024", "07")) // extra value
                .withStorage(storageBuilder ->
                        storageBuilder.setLocation("s3://bucket/test_path/year=2024")
                                .setStorageFormat(StorageFormat.NULL_STORAGE_FORMAT))
                .build();

        assertThatThrownBy(() -> HudiUtil.getHivePartitionName(partition))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("mismatch between column names and values");
    }
}
