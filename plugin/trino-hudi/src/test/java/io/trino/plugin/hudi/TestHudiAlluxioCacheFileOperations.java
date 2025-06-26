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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getCacheOperationSpans;
import static io.trino.filesystem.tracing.CacheFileSystemTraceUtils.getFileLocation;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.COMMIT_METADATA;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.DATA;
import static io.trino.plugin.hudi.util.FileOperationUtils.FileType.HOODIE_PROPERTIES;
import static io.trino.testing.MultisetAssertions.assertMultisetsEqual;
import static java.util.stream.Collectors.toCollection;

@Execution(ExecutionMode.SAME_THREAD)
public class TestHudiAlluxioCacheFileOperations
        extends AbstractTestQueryFramework
{
    private Path cacheDirectory;

    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        cacheDirectory = Files.createTempDirectory("cache");
        closeAfterClass(() -> deleteRecursively(cacheDirectory, ALLOW_INSECURE));

        Map<String, String> hudiProperties = ImmutableMap.<String, String>builder()
                .put("fs.cache.enabled", "true")
                .put("fs.cache.directories", cacheDirectory.toAbsolutePath().toString())
                .put("fs.cache.max-sizes", "100MB")
                .put("hudi.metadata-cache.enabled", "false")
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
                        .add(new CacheOperation("Input.readFully", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Alluxio.readCached", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Alluxio.writeCache", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Alluxio.readExternalStream", COMMIT_METADATA))
                        .add(new CacheOperation("InputFile.length", COMMIT_METADATA))
                        .add(new CacheOperation("Alluxio.readCached", COMMIT_METADATA))
                        .add(new CacheOperation("Alluxio.writeCache", COMMIT_METADATA))
                        .add(new CacheOperation("Alluxio.readCached", DATA))
                        .add(new CacheOperation("InputFile.length", DATA))
                        .add(new CacheOperation("Input.readFully", DATA))
                        .add(new CacheOperation("Alluxio.writeCache", DATA))
                        .build());

        assertFileSystemAccesses(
                "SELECT * FROM hudi.default.stock_ticks_cow",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Alluxio.readCached", COMMIT_METADATA))
                        .add(new CacheOperation("InputFile.length", COMMIT_METADATA))
                        .add(new CacheOperation("Alluxio.readCached", DATA))
                        .add(new CacheOperation("InputFile.length", DATA))
                        .build());
    }

    @Test
    public void testSelectWithFilter()
    {
        assertFileSystemAccesses(
                "SELECT * FROM hudi.default.stock_ticks_cow WHERE symbol = 'GOOG'",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Input.readFully", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Alluxio.readCached", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Alluxio.writeCache", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Alluxio.readExternalStream", COMMIT_METADATA))
                        .add(new CacheOperation("InputFile.length", COMMIT_METADATA))
                        .add(new CacheOperation("Alluxio.readCached", COMMIT_METADATA))
                        .add(new CacheOperation("Alluxio.writeCache", COMMIT_METADATA))
                        .add(new CacheOperation("Alluxio.readCached", DATA))
                        .add(new CacheOperation("InputFile.length", DATA))
                        .add(new CacheOperation("Input.readFully", DATA))
                        .add(new CacheOperation("Alluxio.writeCache", DATA))
                        .build());

        assertFileSystemAccesses(
                "SELECT * FROM hudi.default.stock_ticks_cow WHERE symbol = 'GOOG'",
                ImmutableMultiset.<CacheOperation>builder()
                        .add(new CacheOperation("Alluxio.readCached", HOODIE_PROPERTIES))
                        .add(new CacheOperation("Alluxio.readCached", COMMIT_METADATA))
                        .add(new CacheOperation("InputFile.length", COMMIT_METADATA))
                        .add(new CacheOperation("Alluxio.readCached", DATA))
                        .add(new CacheOperation("InputFile.length", DATA))
                        .build());
    }

    @Test
    public void testJoin()
    {
        assertFileSystemAccesses("SELECT * FROM hudi.default.stock_ticks_cow t1 JOIN hudi.default.stock_ticks_cow t2 ON t1.symbol = t2.symbol",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Input.readFully", HOODIE_PROPERTIES), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", HOODIE_PROPERTIES), 2)
                        .addCopies(new CacheOperation("Alluxio.writeCache", HOODIE_PROPERTIES), 2)
                        .addCopies(new CacheOperation("Alluxio.readExternalStream", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("InputFile.length", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("Alluxio.writeCache", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", DATA), 2)
                        .addCopies(new CacheOperation("InputFile.length", DATA), 2)
                        .addCopies(new CacheOperation("Input.readFully", DATA), 2)
                        .addCopies(new CacheOperation("Alluxio.writeCache", DATA), 2)
                        .build());

        assertFileSystemAccesses("SELECT * FROM hudi.default.stock_ticks_cow t1 JOIN hudi.default.stock_ticks_cow t2 ON t1.symbol = t2.symbol",
                ImmutableMultiset.<CacheOperation>builder()
                        .addCopies(new CacheOperation("Alluxio.readCached", HOODIE_PROPERTIES), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("InputFile.length", COMMIT_METADATA), 2)
                        .addCopies(new CacheOperation("Alluxio.readCached", DATA), 2)
                        .addCopies(new CacheOperation("InputFile.length", DATA), 2)
                        .build());
    }

    private void assertFileSystemAccesses(@Language("SQL") String query, Multiset<CacheOperation> expectedCacheAccesses)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(queryRunner.getDefaultSession(), query);
        assertMultisetsEqual(getCacheOperations(), expectedCacheAccesses);
    }

    private Multiset<CacheOperation> getCacheOperations()
    {
        return getCacheOperationSpans(getQueryRunner())
                .stream()
                .filter(span -> !span.getName().startsWith("InputFile.newStream"))
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