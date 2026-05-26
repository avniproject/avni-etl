package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AttendanceTypeTable extends Table {

    @Override
    public String name(Map<String, Object> tableDetails) {
        return "attendance_type";
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                        new Column("subject_type_uuid", Column.Type.text, Column.ColumnType.index),
                        new Column("name", Column.Type.text),
                        new Column("sort_order", Column.Type.integer),
                        new Column("config", Column.Type.jsonb)
                ))
                .build();
    }
}
