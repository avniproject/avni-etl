package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GroupToMemberTable extends Table {

    @Override
    public String name(Map<String, Object> tableDetails) {
        return generateTableName("Registration", null, tableDetails, "group_subject_type_name", "member_subject_type_name");
    }

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                        new Column("group_subject_id", Column.Type.integer, Column.ColumnType.index),
                        new Column("member_subject_id", Column.Type.integer, Column.ColumnType.index),
                        new Column("role", Column.Type.text),
                        new Column("created_by_username", Column.Type.text),
                        new Column("last_modified_by_username", Column.Type.text)
                ))
                .build();
    }
}
