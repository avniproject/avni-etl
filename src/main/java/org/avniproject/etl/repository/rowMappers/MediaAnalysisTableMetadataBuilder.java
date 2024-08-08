package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.MediaAnalysisTable;

import java.util.stream.Collectors;

public class MediaAnalysisTableMetadataBuilder {
    public static TableMetadata build() {
        TableMetadata mediaAnalysisTableMetadata = new TableMetadata();
        MediaAnalysisTable mediaAnalysisTable = new MediaAnalysisTable();
        mediaAnalysisTableMetadata.setName(mediaAnalysisTable.name(null));
        mediaAnalysisTableMetadata.setType(TableMetadata.Type.MediaAnalysis);
        mediaAnalysisTableMetadata.addColumnMetadata(mediaAnalysisTable.columns().stream().map(column -> new ColumnMetadata(column, null, null, null)).collect(Collectors.toList()));

        return mediaAnalysisTableMetadata;
    }
}
