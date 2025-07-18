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
import io.trino.spi.connector.SchemaTableName;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class HudiBaseIndexSupport
        implements HudiIndexSupport
{
    private final Logger log;
    protected final SchemaTableName schemaTableName;
    protected final Lazy<HoodieTableMetaClient> lazyMetaClient;

    public HudiBaseIndexSupport(Logger log, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient)
    {
        this.log = requireNonNull(log, "log is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.lazyMetaClient = requireNonNull(lazyMetaClient, "metaClient is null");
    }

    public void printDebugMessage(Map<String, List<FileSlice>> candidateFileSlices, Map<String, List<FileSlice>> inputFileSlices, long lookupDurationMs)
    {
        if (log.isDebugEnabled()) {
            int candidateFileSize = candidateFileSlices.values().stream().mapToInt(List::size).sum();
            int totalFiles = inputFileSlices.values().stream().mapToInt(List::size).sum();
            double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles * 1.0d);

            log.info("Total files: %s; files after data skipping: %s; skipping percent %s; time taken: %s ms; table name: %s",
                    totalFiles,
                    candidateFileSize,
                    skippingPercent,
                    lookupDurationMs,
                    schemaTableName);
        }
    }

    protected Map<String, HoodieIndexDefinition> getAllIndexDefinitions()
    {
        if (lazyMetaClient.get().getIndexMetadata().isEmpty()) {
            return Map.of();
        }

        return lazyMetaClient.get().getIndexMetadata().get().getIndexDefinitions();
    }
}
