package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CalendarDateMarkerTable extends Table {

    @Override
    public String name(Map<String, Object> tableDetails) {
        return "calendar_date_marker";
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                        new Column("calendar_uuid", Column.Type.text, Column.ColumnType.index),
                        new Column("marker_date", Column.Type.date),
                        new Column("name", Column.Type.text),
                        new Column("is_working", Column.Type.bool)
                ))
                .build();
    }
}
