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
import io.airlift.units.Duration;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.expression.HudiColumnStatsIndexEvaluator;
import io.trino.plugin.hudi.expression.HudiTrinoFunctionExpression;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hudi.HudiSessionProperties.getExpressionIndexWaitTimeout;

public class HudiExpressionIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiExpressionIndexSupport.class);
    private final List<Predicate> predicates;
    private final Map<HoodieIndexDefinition, Expression> applicableIndexDefinitions;
    private final CompletableFuture<Optional<ExpressionIndexStats>> expressionIndexStatsFuture;
    private final Duration expressionIndexWaitTimeout;
    private final long futureStartTimeMs;

    public HudiExpressionIndexSupport(ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, HudiTableHandle tableHandle)
    {
        super(log, schemaTableName, lazyMetaClient);
        this.predicates = tableHandle.getExpressionIndexCandidates();
        this.applicableIndexDefinitions = getApplicableIndexDefinitions();
        this.expressionIndexWaitTimeout = getExpressionIndexWaitTimeout(session);

        this.expressionIndexStatsFuture = CompletableFuture.supplyAsync(() -> {
            HoodieTimer timer = HoodieTimer.start();
            for (Map.Entry<HoodieIndexDefinition, Expression> defExprEntry : this.applicableIndexDefinitions.entrySet()) {
                List<String> encodedTargetColumnNames = defExprEntry.getKey().getSourceFields()
                        .stream()
                        .map(col -> new ColumnIndexID(col).asBase64EncodedString()).toList();

                // HoodieMetadataColumnStats from all partitions (including PartitionStats) will be read out
                // If partition pruning is handled before index is called, this will be fine for now
                // TODO: Figure out how we can pass in LazyPartitions to reduce number of HoodieMetadataColumnStats loaded
                String indexName = defExprEntry.getKey().getIndexName();
                Map<String, List<HoodieMetadataColumnStats>> statsByFileName = lazyTableMetadata.get()
                        .getRecordsByKeyPrefixes(encodedTargetColumnNames, indexName, true)
                        .collectAsList()
                        .stream()
                        .filter(f -> f.getData().getColumnStatMetadata().isPresent())
                        .map(f -> f.getData().getColumnStatMetadata().get())
                        .collect(Collectors.groupingBy(HoodieMetadataColumnStats::getFileName));

                log.debug("Expression stats lookup took %s ms and identified %d relevant file IDs.", timer.endTimer(), statsByFileName.size());

                // TODO: Use all applicable Expressions
                return Optional.of(ExpressionIndexStats.of(statsByFileName, defExprEntry.getValue()));
            }
            return Optional.empty();
        });

        this.futureStartTimeMs = System.currentTimeMillis();
    }

    @Override
    public boolean shouldSkipFileSlice(FileSlice slice)
    {
        try {
            HudiColumnStatsIndexEvaluator columnStatsIndexEvaluator = new HudiColumnStatsIndexEvaluator();

            if (expressionIndexStatsFuture.isDone()) {
                Optional<ExpressionIndexStats> expressionIndexStatsOpt = expressionIndexStatsFuture.get();
                return expressionIndexStatsOpt
                        .map(stats -> shouldSkipFileSlice(slice, stats.getStatsByFileName(), columnStatsIndexEvaluator, stats.getExpression()))
                        .orElse(false);
            }

            long elapsedMs = System.currentTimeMillis() - futureStartTimeMs;
            if (elapsedMs > expressionIndexWaitTimeout.toMillis()) {
                // Took too long; skip decision
                return false;
            }

            // If still within the timeout window, wait up to the remaining time
            long remainingMs = Math.max(0, expressionIndexWaitTimeout.toMillis() - elapsedMs);
            Optional<ExpressionIndexStats> expressionIndexStatsOpt =
                    expressionIndexStatsFuture.get(remainingMs, TimeUnit.MILLISECONDS);

            return expressionIndexStatsOpt
                    .map(stats -> shouldSkipFileSlice(slice, stats.getStatsByFileName(), columnStatsIndexEvaluator, stats.getExpression()))
                    .orElse(false);
        }
        catch (TimeoutException | InterruptedException | ExecutionException e) {
            return false;
        }
    }

    private boolean shouldSkipFileSlice(FileSlice fileSlice,
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

        List<HoodieMetadataColumnStats> stats = statsByFileName.get(fileSliceName);
        checkArgument(stats.size() == 1, "Map should only have one element (All expressions supported only use one column as an argument)");
        // Ensure that the column stats to be used for evaluation is set
        columnStatsIndexEvaluator.setStats(stats.getFirst());
        return !expression.accept(columnStatsIndexEvaluator);
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

    static class ExpressionIndexStats
    {
        private final Map<String, List<HoodieMetadataColumnStats>> statsByFileName;
        private final Expression expression;

        private ExpressionIndexStats(Map<String, List<HoodieMetadataColumnStats>> statsByFileName, Expression expression)
        {
            this.statsByFileName = statsByFileName;
            this.expression = expression;
        }

        public static ExpressionIndexStats of(Map<String, List<HoodieMetadataColumnStats>> statsByFileName, Expression expression)
        {
            return new ExpressionIndexStats(statsByFileName, expression);
        }

        public Map<String, List<HoodieMetadataColumnStats>> getStatsByFileName()
        {
            return statsByFileName;
        }

        public Expression getExpression()
        {
            return expression;
        }
    }
}
