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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.plugin.hudi.util.FileOperationUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.Map;

import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getFileLocation;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.isTrinoSchemaOrPermissions;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.COMMIT_METADATA;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.DATA;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.HOODIE_PROPERTIES;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.stream.Collectors.toCollection;

@Execution(ExecutionMode.SAME_THREAD)
public class TestHudiMemoryCacheFileOperations
        extends AbstractTestQueryFramework
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        Map<String, String> hudiProperties = ImmutableMap.<String, String>builder()
                .put("hudi.metadata-cache.enabled", "true")
                .buildOrThrow();

        return HudiQueryRunner.builder()
                .setExtraProperties(Map.of("http-server.http.port", "8080"))
                .setHudiProperties(hudiProperties)
                .setTablesInitializer(new ResourceHudiTablesInitializer())
                .setWorkerCount(0)
                .build();
    }

    @Test
    public void testCacheFileOperations()
    {
        assertFileSystemAccesses(
                "SELECT * FROM hudi.default.stock_ticks_cow",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Input.readTail", HOODIE_PROPERTIES))
                        .add(new CacheOperation("FileSystemCache.cacheStream", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Input.readTail", COMMIT_METADATA))
                        .add(new CacheOperation("FileSystemCache.cacheStream", COMMIT_METADATA))
                        .add(new CacheOperation("Input.readTail", DATA))
                        .add(new CacheOperation("FileSystemCache.cacheInput", DATA))
                        .build());

        assertFileSystemAccesses(
                "SELECT * FROM hudi.default.stock_ticks_cow",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("FileSystemCache.cacheStream", HOODIE_PROPERTIES))
                        .add(new CacheOperation("FileSystemCache.cacheStream", COMMIT_METADATA))
                        .add(new CacheOperation("FileSystemCache.cacheInput", DATA))
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertFileSystemAccesses(
                "SELECT * FROM hudi.default.stock_ticks_cow WHERE symbol = 'GOOG'",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("FileSystemCache.cacheStream", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Input.readTail", HOODIE_PROPERTIES))
                        .add(new CacheOperation("FileSystemCache.cacheStream", COMMIT_METADATA))
                        .add(new CacheOperation("Input.readTail", COMMIT_METADATA))
                        .add(new CacheOperation("FileSystemCache.cacheInput", DATA))
                        .add(new CacheOperation("Input.readTail", DATA))
                        .build());

        assertFileSystemAccesses(
                "SELECT * FROM hudi.default.stock_ticks_cow WHERE symbol = 'GOOG'",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("FileSystemCache.cacheStream", HOODIE_PROPERTIES))
                        .add(new CacheOperation("FileSystemCache.cacheStream", COMMIT_METADATA))
                        .add(new CacheOperation("FileSystemCache.cacheInput", DATA))
                        .build());
    }

    @Test
    public void testJoin()
    {
        assertFileSystemAccesses("SELECT * FROM hudi.default.stock_ticks_cow t1 JOIN hudi.default.stock_ticks_cow t2 ON t1.symbol = t2.symbol",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Input.readTail", HOODIE_PROPERTIES), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", HOODIE_PROPERTIES), 2)
                        .addCopies(new CacheOperation("Input.readTail", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("Input.readTail", DATA), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheInput", DATA), 2)
                        .build());

        assertFileSystemAccesses("SELECT * FROM hudi.default.stock_ticks_cow t1 JOIN hudi.default.stock_ticks_cow t2 ON t1.symbol = t2.symbol",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", HOODIE_PROPERTIES), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheStream", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("FileSystemCache.cacheInput", DATA), 2)
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<CacheOperation> expectedCacheAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(expectedCacheAccesses, getCacheOperations());
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return getQueryRunner().getSpans().stream()
                .filter(span -> span.getName().startsWith("Input.") || span.getName().startsWith("InputFile.") || span.getName().startsWith("FileSystemCache."))
                .filter(span -> !span.getName().startsWith("InputFile.newInput"))
                .filter(span -> !isTrinoSchemaOrPermissions(getFileLocation(span)))
                .map(CacheOperation::create)
                .collect(toCollection(HashMultiset::create));
    }

    private record CacheOperation(String operationName, FileOperationUtils.FileType fileType)
    {
        public static CacheOperation create(SpanData span)
        {
            String path = getFileLocation(span);
            return new CacheOperation(span.getName(), FileOperationUtils.FileType.fromFilePath(path));
        }
    }
}