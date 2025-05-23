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
package io.trino.plugin.hudi.testing;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.google.common.io.Resources.getResource;
import static java.util.Objects.requireNonNull;

public class HudiTableUnzipper
{
    private HudiTableUnzipper() {}

    /**.
     * <p>This function expects a ZIP archive, whose name is derived from the {@code resourceName} constant (e.g., if {@code resourceName} is "hudi-testing-data",
     * the expected file is "hudi-testing-data.zip"), to be present in the {@code src/test/resources} directory.</p>
     *
     * <p>Initial {@code src/test/resources} Structure:</p>
     * <pre>
     * src/test/resources/
     * ├── hudi-testing-data.zip
     * └── README.md
     * </pre>
     *
     * <p>The method locates the specified ZIP archive (e.g., "hudi-testing-data.zip") within the classpath resources. It then unzips this archive. The contents are extracted into
     * a new directory that is created directly within the root of the classpath's resource folder.</p>
     *
     * <p>The name of this newly created directory will be identical to the {@code resourceName} (e.g., "hudi-testing-data").</p>
     *
     * <p>The destination path (the root resource folder) is determined by resolving {@code getResource("")} to a file system path (via {@code Path.of(getResource("").toURI())}).
     * i.e. to the root of the classpath's resource directory.</p>
     *
     * <p>After this method executes, the directory structure will be:</p>
     * <pre>
     * target/test-classes/
     * ├── hudi-testing-data.zip
     * ├── hudi-testing-data
     * │   ├── hudi-table-1
     * │   └── hudi-table-2
     * └── README.md
     * </pre>
     *
     * @param resourceName The name of the zip file in src/test/resources.
     * @throws IOException If an I/O error occurs.
     */
    public static void unzipResource(String resourceName)
            throws IOException, URISyntaxException
    {
        requireNonNull(resourceName, "Resource name cannot be null or empty.");

        String resourceNameWithExt = resourceName + ".zip";
        URL resourceUrl = HudiTableUnzipper.class.getClassLoader().getResource(resourceNameWithExt);
        if (resourceUrl == null) {
            throw new IOException("Resource not found: " + resourceNameWithExt);
        }
        Path targetDirectory = Path.of(getResource("").toURI());
        if (!Files.exists(targetDirectory)) {
            Files.createDirectories(targetDirectory);
        }
        else if (!Files.isDirectory(targetDirectory)) {
            throw new IOException("Target path exists but is not a directory: " + targetDirectory);
        }

        try (InputStream is = resourceUrl.openStream(); ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry zipEntry = zis.getNextEntry();
            byte[] buffer = new byte[1024];

            while (zipEntry != null) {
                Path newFilePath = targetDirectory.resolve(zipEntry.getName());

                // Prevent Zip Slip vulnerability (Do not want files written outside the our target dir)
                if (!newFilePath.normalize().startsWith(targetDirectory.normalize())) {
                    throw new IOException("Bad zip entry: " + zipEntry.getName());
                }

                if (zipEntry.isDirectory()) {
                    // Handle directories
                    if (!Files.exists(newFilePath)) {
                        Files.createDirectories(newFilePath);
                    }
                }
                else {
                    // Ensure parent directory exists before handling files
                    Path parentDir = newFilePath.getParent();
                    if (parentDir != null && !Files.exists(parentDir)) {
                        Files.createDirectories(parentDir);
                    }

                    try (FileOutputStream fos = new FileOutputStream(newFilePath.toFile())) {
                        int len;
                        while ((len = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, len);
                        }
                    }
                }
                zis.closeEntry();
                zipEntry = zis.getNextEntry();
            }
        }
    }

    /**
     * <p>This method attempts to locate a directory directly as a classpath resource using {@code getResource(TEST_RESOURCE_NAME)} (e.g., if {@code TEST_RESOURCE_NAME} is
     * "hudi-testing-data"). If found, the directory and all its contents are recursively deleted.
     *
     * @param directory The path to the directory to delete.
     * @throws IOException If an I/O error occurs.
     */
    public static void deleteDirectory(String directory)
            throws IOException, URISyntaxException
    {
        Path directoryPath = Path.of(getResource(directory).toURI());
        if (Files.exists(directoryPath)) {
            try (Stream<Path> filesInDir = Files.walk(directoryPath)) {
                filesInDir.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }
}
