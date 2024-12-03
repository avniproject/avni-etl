package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.SyncTelemetryTable;

import java.util.stream.Collectors;

public class SyncTelemetryTableMetadataBuilder {
    public static TableMetadata build() {
        TableMetadata syncTelemetryTableMetadata = new TableMetadata();
        SyncTelemetryTable syncTelemetryTable = new SyncTelemetryTable();
        syncTelemetryTableMetadata.setName(syncTelemetryTable.name(null));
        syncTelemetryTableMetadata.setType(TableMetadata.Type.SyncTelemetry);
        syncTelemetryTableMetadata.addColumnMetadata(syncTelemetryTable.columns().stream().map(column -> new ColumnMetadata(column, null, null, null, false)).collect(Collectors.toList()));

        return syncTelemetryTableMetadata;
    }
}
