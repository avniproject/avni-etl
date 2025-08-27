package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SubjectTable extends Table {
    @Override
    public String name(Map<String, Object> tableDetails) {
        return generateTableName("Registration", null, tableDetails, "subject_type_name");
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                        new Column("address_id", Column.Type.integer, Column.ColumnType.index),
                        new Column("registration_date", Column.Type.date, Column.ColumnType.index),
                        new Column("first_name", Column.Type.text),
                        new Column("last_name", Column.Type.text),
                        new Column("registration_location", Column.Type.point),
                        new Column("legacy_id", Column.Type.text),
                        new Column("latest_approval_status", Column.Type.text),
                        new Column("created_by_username", Column.Type.text),
                        new Column("last_modified_by_username", Column.Type.text)
                ))
                .build();
    }
}
