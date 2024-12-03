package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.Column;

public class ChangeDataType implements Diff {
    private final String tableName;
    private final String columnName;
    private final Column.Type newDataType;

    public ChangeDataType(String tableName, String columnName, Column.Type newDataType) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.newDataType = newDataType;
    }

    @Override
    public String getSql() {
        String sql = """
                alter table "%s"."%s" alter column "%s" type %s;""".formatted(OrgIdentityContextHolder.getDbSchema(), tableName, columnName, newDataType.toString());
        return sql.trim();
    }
}
