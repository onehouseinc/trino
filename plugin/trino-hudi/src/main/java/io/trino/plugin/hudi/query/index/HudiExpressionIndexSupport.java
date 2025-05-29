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
import io.trino.plugin.hudi.expression.HudiColumnStatsIndexEvaluator;
import io.trino.plugin.hudi.expression.HudiTrinoFunctionExpression;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.common.util.hash.PartitionIndexID;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HudiExpressionIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiExpressionIndexSupport.class);
    private final List<Predicate> predicates;
    private final Map<HoodieIndexDefinition, Expression> applicableIndexDefinitions;

    public HudiExpressionIndexSupport(SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, List<Predicate> predicates)
    {
        super(log, schemaTableName, lazyMetaClient);
        this.predicates = predicates;
        applicableIndexDefinitions = getApplicableIndexDefinitions();
    }

    @Override
    public Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(HoodieTableMetadata metadataTable, Map<String, List<FileSlice>> inputFileSlices, TupleDomain<String> regularColumnPredicates)
    {
        HoodieTimer timer = HoodieTimer.start();

        HudiColumnStatsIndexEvaluator columnStatsIndexEvaluator = new HudiColumnStatsIndexEvaluator();
        List<String> prunedPartitions = new ArrayList<>(inputFileSlices.keySet());
        Map<HoodieIndexDefinition, Expression> indexDefinitions = getApplicableIndexDefinitions();
        for (Map.Entry<HoodieIndexDefinition, Expression> defExprEntry : indexDefinitions.entrySet()) {
            List<String> encodedTargetColumnNames = defExprEntry.getKey().getSourceFields()
                    .stream()
                    .map(col -> new ColumnIndexID(col).asBase64EncodedString()).toList();

            List<String> finalEncodedTargetColumnNames;
            if (!prunedPartitions.isEmpty()) {
                // Embed partition information concat(columnName, partitionPath) if possible
                finalEncodedTargetColumnNames = prunedPartitions.stream().map(partitionPath ->
                                new PartitionIndexID(HoodieTableMetadataUtil.getPartitionIdentifier(partitionPath)).asBase64EncodedString())
                        .flatMap(encodedPartition -> encodedTargetColumnNames.stream()
                                .map(encodedTargetColumn -> encodedTargetColumn.concat(encodedPartition)))
                        .toList();
            }
            else {
                finalEncodedTargetColumnNames = encodedTargetColumnNames;
            }

            String indexName = defExprEntry.getKey().getIndexName();
            Map<String, List<HoodieMetadataColumnStats>> statsByFileName = metadataTable
                    .getRecordsByKeyPrefixes(finalEncodedTargetColumnNames, indexName, true)
                    .collectAsList()
                    .stream()
                    .filter(f -> f.getData().getColumnStatMetadata().isPresent())
                    .map(f -> f.getData().getColumnStatMetadata().get())
                    .collect(Collectors.groupingBy(HoodieMetadataColumnStats::getFileName));

            // Prune files
            Map<String, List<FileSlice>> candidateFileSlices = inputFileSlices
                    .entrySet()
                    .stream()
                    .collect(Collectors
                            .toMap(entry -> entry.getKey(), entry -> entry
                                    .getValue()
                                    .stream()
                                    .filter(fileSlice -> shouldKeepFileSlice(fileSlice, statsByFileName, columnStatsIndexEvaluator, defExprEntry.getValue()))
                                    .collect(Collectors.toList())));

            this.printDebugMessage(candidateFileSlices, inputFileSlices, timer.endTimer());

            // Only use first applicable Expression for now (returns on first iteration)
            // TODO: Use all applicable Expressions
            return candidateFileSlices;
        }

        return inputFileSlices;
    }

    private boolean shouldKeepFileSlice(FileSlice fileSlice,
            Map<String, List<HoodieMetadataColumnStats>> statsByFileName,
            HudiColumnStatsIndexEvaluator columnStatsIndexEvaluator,
            Expression expression)
    {
        // Only support column stats for now
        String fileSliceName = fileSlice.getBaseFile().map(BaseFile::getFileName).orElse("");
        // If no stats exist for this specific file, we cannot prune it.
        if (!statsByFileName.containsKey(fileSliceName)) {
            return true;
        }

        // List should only have one element (All expressions supported only use one column as an argument)
        List<HoodieMetadataColumnStats> stats = statsByFileName.get(fileSliceName);
        HoodieMetadataColumnStats currentColumnStats = stats.getFirst();
        // Ensure that the column stats to be used for evaluation is set
        columnStatsIndexEvaluator.setStats(currentColumnStats);
        return expression.accept(columnStatsIndexEvaluator);
    }

    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        if (applicableIndexDefinitions.isEmpty()) {
            log.debug("No applicable secondary index definitions found.");
            return false;
        }
        return true;
    }

    /**
     * Identifies expression index definitions that are applicable to the current set of predicates.
     *
     * @return A map of applicable expression index names to their definitions.
     */
    private Map<HoodieIndexDefinition, Expression> getApplicableIndexDefinitions()
    {
        // TODO: Convert this to use a visitor pattern
        Map<String, HoodieIndexDefinition> columnToIndexDefinition = getAllIndexDefinitions()
                .entrySet().stream()
                .filter(e -> e.getKey().contains(HoodieTableMetadataUtil.PARTITION_NAME_EXPRESSION_INDEX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<HoodieIndexDefinition, Expression> usableCandidates = new HashMap<>();
        for (Predicate predicate : predicates) {
            if (predicate instanceof Predicates.Not notPredicate) {
                // Will only have one expression
                notPredicate.getChildren().getFirst();
            }
            else if (predicate instanceof Predicates.BinaryComparison binaryComparison) {
                for (HoodieIndexDefinition indexDefinition : columnToIndexDefinition.values()) {
                    boolean canUseIndex = canUseIndex(predicate, indexDefinition);
                    if (canUseIndex) {
                        usableCandidates.put(indexDefinition, binaryComparison);
                    }
                }
            }
        }
        return usableCandidates;
    }

    public static boolean canUseIndex(Predicate predicate, HoodieIndexDefinition indexDefinition)
    {
        if (indexDefinition == null || indexDefinition.getIndexFunction().isEmpty()) {
            return false;
        }
        if (predicate instanceof Predicates.BinaryComparison binaryComparison) {
            Expression leftOperand = binaryComparison.getLeft();
            Expression rightOperand = binaryComparison.getRight();
            // The right operand must be a literal
            if (!(rightOperand instanceof Literal)) {
                return false;
            }
            if (leftOperand instanceof HudiTrinoFunctionExpression funcExpr) {
                return funcExpr.canUseIndex(indexDefinition);
            }
            return false;
        }
        return false;
    }
}
