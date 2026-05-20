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
package io.trino.plugin.hudi.query.index;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.HudiConfig;
import io.trino.plugin.hudi.HudiSessionProperties;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.testing.TestingConnectorSession;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiColumnStatsIndexSupport
{
    private static final SchemaTableName SCHEMA_TABLE = new SchemaTableName("schema", "table");

    @Test
    public void testScopedFlagOnFallsBackWhenPrunedPathsNotPublished()
    {
        // With the scoped flag on, the constructor does not start the lookup; it waits for
        // setPrunedPartitionPaths to publish the pruned set. If that never happens (e.g. the
        // caller forgot to wire it up), shouldSkipFileSlice must fall back to "no skip"
        // rather than block forever or throw.
        HudiColumnStatsIndexSupport support = new HudiColumnStatsIndexSupport(
                sessionWithScopeFlag(true),
                SCHEMA_TABLE,
                failingMetaClient(),
                failingMetadata(),
                singleColumnPredicate());

        assertThat(support.shouldSkipFileSlice(newFileSlice())).isFalse();
    }

    @Test
    public void testBuildColumnPartitionPrefixKeysCombinesColumnAndPartitionIds()
    {
        HudiColumnStatsIndexSupport support = new HudiColumnStatsIndexSupport(
                sessionWithScopeFlag(true),
                SCHEMA_TABLE,
                failingMetaClient(),
                failingMetadata(),
                singleColumnPredicate());

        List<String> partitions = ImmutableList.of("year=2024", "year=2025");
        List<String> rawKeys = support.buildColumnPartitionPrefixKeys(partitions);

        String columnPart = new ColumnIndexID("col1").asBase64EncodedString();
        String expected2024 = columnPart + new PartitionIndexID(
                HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier("year=2024")).asBase64EncodedString();
        String expected2025 = columnPart + new PartitionIndexID(
                HoodieTableMetadataUtil.getColumnStatsIndexPartitionIdentifier("year=2025")).asBase64EncodedString();
        assertThat(rawKeys).containsExactly(expected2024, expected2025);
    }

    @Test
    public void testNoPredicatesShortCircuitsWithoutMetadataAccess()
    {
        // With no regular column predicates, the constructor publishes a completed empty
        // future regardless of the flag. shouldSkipFileSlice must never skip, and must not
        // touch the table metadata.
        HudiColumnStatsIndexSupport support = new HudiColumnStatsIndexSupport(
                sessionWithScopeFlag(true),
                SCHEMA_TABLE,
                failingMetaClient(),
                failingMetadata(),
                TupleDomain.all());

        assertThat(support.shouldSkipFileSlice(newFileSlice())).isFalse();
    }

    @Test
    public void testFlagOffSetPrunedPartitionPathsIsNoOp()
    {
        // With the flag off, setPrunedPartitionPaths must not kick off a deferred lookup.
        // Combined with TupleDomain.all() (constructor sets a completed empty future), this
        // guarantees no metadata access regardless of whether pruned paths are published.
        HudiColumnStatsIndexSupport support = new HudiColumnStatsIndexSupport(
                sessionWithScopeFlag(false),
                SCHEMA_TABLE,
                failingMetaClient(),
                failingMetadata(),
                TupleDomain.all());

        support.setPrunedPartitionPaths(ImmutableList.of("year=2024"));
        assertThat(support.shouldSkipFileSlice(newFileSlice())).isFalse();
    }

    private static ConnectorSession sessionWithScopeFlag(boolean scopeToPrunedPartitions)
    {
        HudiConfig config = new HudiConfig().setScopeColumnStatsToPrunedPartitions(scopeToPrunedPartitions);
        HudiSessionProperties sessionProperties = new HudiSessionProperties(config, new ParquetReaderConfig());
        return TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
    }

    private static Lazy<HoodieTableMetaClient> failingMetaClient()
    {
        return Lazy.lazily(() -> { throw new AssertionError("metaClient must not be accessed"); });
    }

    private static Lazy<HoodieTableMetadata> failingMetadata()
    {
        return Lazy.lazily(() -> { throw new AssertionError("tableMetadata must not be accessed"); });
    }

    private static TupleDomain<String> singleColumnPredicate()
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of("col1", singleValue(BIGINT, 42L)));
    }

    private static FileSlice newFileSlice()
    {
        return new FileSlice("partition", "0", "file-1");
    }
}
