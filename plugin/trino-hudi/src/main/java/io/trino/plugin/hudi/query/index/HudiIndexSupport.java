package io.trino.plugin.hudi.query.index;

import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.util.List;
import java.util.Map;

public interface HudiIndexSupport
{
    Map<String, List<FileSlice>> lookupCandidateFilesInMetadataTable(
            HoodieTableMetadata metadataTable,
            Map<String, List<FileSlice>> inputFileSlices,
            TupleDomain<String> regularColumnPredicates);
}
