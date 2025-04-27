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

import io.airlift.log.Logger;
import io.trino.plugin.hudi.util.TupleDomainUtils;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.util.TupleDomainUtils.hasSimpleNullCheck;

public class HudiPartitionStatsIndexSupport
        extends HudiColumnStatsIndexSupport
{
    private static final Logger log = Logger.get(HudiColumnStatsIndexSupport.class);

    public HudiPartitionStatsIndexSupport(HoodieTableMetaClient metaClient)
    {
        super(log, metaClient);
    }

    @Override
    public Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(HoodieTableMetadata metadataTable, Map<String, List<FileSlice>> inputFileSlices, TupleDomain<String> regularColumnPredicates)
    {
        return Map.of();
    }

    public Optional<List<String>> prunePartitions(HoodieTableMetadata metadataTable, TupleDomain<String> regularColumnPredicates)
    {
        // Filter out predicates containing simple null checks (`IS NULL` or `IS NOT NULL`)
        TupleDomain<String> filteredRegularPredicates = regularColumnPredicates.filter((_, domain) -> !hasSimpleNullCheck(domain));

        // Sanity check, if no regular domains, return immediately
        if (filteredRegularPredicates.getDomains().isEmpty()) {
            return Optional.empty();
        }

        List<String> regularColumns = new ArrayList<>(filteredRegularPredicates.getDomains().get().keySet());

        // Get columns to filter on
        List<String> encodedTargetColumnNames = regularColumns
                .stream()
                .map(col -> new ColumnIndexID(col).asBase64EncodedString()).toList();

        // Map of partition stats keyed by partition name
        // If some columns in encodedTargetColumnNames is not available in partition stats index, partition will not be pruned
        // This is so as we cannot be sure that the constraint is true or not, hence, adopt conservative measure
        Map<String, List<HoodieMetadataColumnStats>> statsByPartitionName = metadataTable.getRecordsByKeyPrefixes(
                        encodedTargetColumnNames,
                        HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS, true)
                .collectAsList()
                .stream()
                .filter(f -> f.getData().getColumnStatMetadata().isPresent())
                .map(f -> f.getData().getColumnStatMetadata().get())
                .collect(Collectors.groupingBy(HoodieMetadataColumnStats::getFileName));

        List<String> prunedPartitions = statsByPartitionName.entrySet()
                .stream()
                .filter(e -> evaluateStatisticPredicate(filteredRegularPredicates, e.getValue(), regularColumns)).map(e -> e.getKey())
                .toList();

        return Optional.of(prunedPartitions);
    }

    public static boolean isIndexSupportAvailable(HoodieTableMetaClient metaClient)
    {
        return metaClient.getTableConfig().getMetadataPartitions().contains(HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS);
    }

    public static boolean shouldUseIndex(TupleDomain<String> tupleDomain, HoodieTableMetaClient metaClient)
    {
        if (!isIndexSupportAvailable(metaClient)) {
            return false;
        }
        List<String> sourceFields = metaClient.getIndexMetadata().get().getIndexDefinitions()
                .get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS).getSourceFields();

        return TupleDomainUtils.areSomeFieldsReferenced(tupleDomain, sourceFields);
    }
}
