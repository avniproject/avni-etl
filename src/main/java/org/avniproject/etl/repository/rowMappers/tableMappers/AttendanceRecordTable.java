package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AttendanceRecordTable extends Table {

    @Override
    public String name(Map<String, Object> tableDetails) {
        return "attendance_record";
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                        new Column("session_uuid", Column.Type.text, Column.ColumnType.index),
                        new Column("subject_uuid", Column.Type.text, Column.ColumnType.index),
                        new Column("status", Column.Type.text),
                        new Column("reason_concept_uuids", Column.Type.jsonb),
                        new Column("other_reason_text", Column.Type.text),
                        new Column("needs_follow_up", Column.Type.bool),
                        new Column("follow_up_encounter_uuid", Column.Type.text)
                ))
                .build();
    }
}
