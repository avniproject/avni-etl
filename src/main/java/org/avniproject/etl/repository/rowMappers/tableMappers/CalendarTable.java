package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CalendarTable extends Table {

    @Override
    public String name(Map<String, Object> tableDetails) {
        return "calendar";
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                        new Column("name", Column.Type.text),
                        new Column("working_pattern", Column.Type.jsonb),
                        new Column("address_level_uuid", Column.Type.text, Column.ColumnType.index),
                        new Column("is_default", Column.Type.bool)
                ))
                .build();
    }
}
