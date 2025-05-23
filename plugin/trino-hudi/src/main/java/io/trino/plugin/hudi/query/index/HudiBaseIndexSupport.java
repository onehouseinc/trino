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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class HudiBaseIndexSupport
        implements HudiIndexSupport
{
    private final Logger log;
    protected final HoodieTableMetaClient metaClient;

    public HudiBaseIndexSupport(Logger log, HoodieTableMetaClient metaClient)
    {
        this.log = requireNonNull(log, "log is null");
        this.metaClient = requireNonNull(metaClient, "metaClient is null");
    }

    public void printDebugMessage(Map<String, List<FileSlice>> candidateFileSlices, Map<String, List<FileSlice>> inputFileSlices)
    {
        if (log.isDebugEnabled()) {
            int candidateFileSize = candidateFileSlices.values().stream().mapToInt(List::size).sum();
            int totalFiles = inputFileSlices.values().stream().mapToInt(List::size).sum();
            double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles * 1.0d);
            log.info("Total files: %s; files after data skipping: %s; skipping percent %s",
                    totalFiles,
                    candidateFileSize,
                    skippingPercent);
        }
    }

    protected Map<String, HoodieIndexDefinition> getAllIndexDefinitions()
    {
        if (metaClient.getIndexMetadata().isEmpty()) {
            return Map.of();
        }

        return metaClient.getIndexMetadata().get().getIndexDefinitions();
    }
}
