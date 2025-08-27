package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class EncounterTable extends Table {

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                                new Column("name", Column.Type.text),
                                new Column("individual_id", Column.Type.integer, Column.ColumnType.index),
                                new Column("address_id", Column.Type.integer, Column.ColumnType.index),
                                new Column("earliest_visit_date_time", Column.Type.timestampWithTimezone),
                                new Column("max_visit_date_time", Column.Type.timestampWithTimezone),
                                new Column("encounter_date_time", Column.Type.timestampWithTimezone, Column.ColumnType.index),
                                new Column("encounter_location", Column.Type.point),
                                new Column("cancel_date_time", Column.Type.timestampWithTimezone, Column.ColumnType.index),
                                new Column("cancel_location", Column.Type.point),
                                new Column("legacy_id", Column.Type.text),
                                new Column("latest_approval_status", Column.Type.text),
                                new Column("filled_by_id", Column.Type.integer, Column.ColumnType.index),
                                new Column("created_by_username", Column.Type.text),
                                new Column("last_modified_by_username", Column.Type.text),
                                new Column("filled_by_username", Column.Type.text)
                        )
                ).build();
    }

    @Override
    public String name(Map<String, Object> tableDetails) {
        return generateTableName("Encounter", null, tableDetails, "subject_type_name", "encounter_type_name");
    }
}
