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
package io.trino.plugin.hudi.query;

import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import org.apache.hudi.common.model.FileSlice;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

public interface HudiDirectoryLister
        extends Closeable
{
    List<FileSlice> listStatus(HudiPartitionInfo partitionInfo, boolean useIndex);

    Optional<HudiPartitionInfo> getPartitionInfo(String partition);
}
