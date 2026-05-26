package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SessionTable extends Table {

    @Override
    public String name(Map<String, Object> tableDetails) {
        return "session";
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                        new Column("group_subject_uuid", Column.Type.text, Column.ColumnType.index),
                        new Column("scheduled_date", Column.Type.date),
                        new Column("attendance_type_uuid", Column.Type.text, Column.ColumnType.index),
                        new Column("status", Column.Type.text),
                        new Column("reason_concept_uuid", Column.Type.text),
                        new Column("notes", Column.Type.text),
                        new Column("marked_by_user_id", Column.Type.integer),
                        new Column("marked_at", Column.Type.timestampWithTimezone)
                ))
                .build();
    }
}
