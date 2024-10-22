package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;

public class CommonColumns {
    public static final Column LastModifiedDateTimeColumn = new Column("last_modified_date_time", Column.Type.timestampWithTimezone);
    public static final Column IsVoidedColumn = new Column("is_voided", Column.Type.bool);
    public static final Column OrganisationIdColumn = new Column("organisation_id", Column.Type.integer, Column.ColumnType.index);
    public static final Column SerialIdColumn = new Column("id", Column.Type.serial, Column.ColumnType.index);

    public static final List<Column> commonColumns = Arrays.asList(
            new Column("uuid", Column.Type.text, Column.ColumnType.index),
            IsVoidedColumn,
            new Column("created_by_id", Column.Type.integer, Column.ColumnType.index),
            new Column("last_modified_by_id", Column.Type.integer, Column.ColumnType.index),
            new Column("created_date_time", Column.Type.timestampWithTimezone, Column.ColumnType.index),
            LastModifiedDateTimeColumn,
            OrganisationIdColumn
        );

    public static final List<Column> CommonRepeatableGroupColumns = List.of(SerialIdColumn, LastModifiedDateTimeColumn, IsVoidedColumn, OrganisationIdColumn);
}
