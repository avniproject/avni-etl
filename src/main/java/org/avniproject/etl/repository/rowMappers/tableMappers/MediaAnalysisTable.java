package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MediaAnalysisTable extends Table {
    @Override
    public String name(Map<String, Object> tableDetails) {
        return "media_analysis";
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withColumns(Arrays.asList(
                        new Column("uuid", Column.Type.text, Column.ColumnType.index),
                        new Column("image_url", Column.Type.text),
                        new Column("is_valid_url", Column.Type.bool),
                        new Column("is_present_in_storage", Column.Type.bool),
                        new Column("is_thumbnail_generated", Column.Type.bool)
                ))
                .build();
    }
}