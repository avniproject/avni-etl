package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.CalendarTable;

import java.util.stream.Collectors;

public class CalendarTableMetadataBuilder {
    public static TableMetadata build() {
        TableMetadata tableMetadata = new TableMetadata();
        CalendarTable table = new CalendarTable();
        tableMetadata.setName(table.name(null));
        tableMetadata.setType(TableMetadata.Type.Calendar);
        tableMetadata.addColumnMetadata(table.columns().stream().map(column -> new ColumnMetadata(column, null, null, null, false)).collect(Collectors.toList()));
        table.columns().stream().filter(Column::isIndexed).forEach(tableMetadata::addIndexMetadata);
        return tableMetadata;
    }
}
