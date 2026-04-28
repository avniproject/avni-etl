package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.avniproject.etl.domain.metadata.diff.Strings.*;

public class AlterColumnType implements Diff {
    private final String tableName;
    private final String columnName;
    private final Column.Type newType;
    private static final Logger log = LoggerFactory.getLogger(AlterColumnType.class);

    public AlterColumnType(String tableName, String columnName, Column.Type newType) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.newType = newType;
    }

    @Override
    public String getSql() {
        String sql = new StringBuffer()
                .append("alter table ")
                .append(QUOTE)
                .append(OrgIdentityContextHolder.getDbSchema())
                .append(QUOTE)
                .append(DOT)
                .append(tableName)
                .append(" alter column ")
                .append(QUOTE)
                .append(columnName)
                .append(QUOTE)
                .append(" type ")
                .append(newType.typeString())
                .append(" using ")
                .append(QUOTE)
                .append(columnName)
                .append(QUOTE)
                .append("::")
                .append(newType.typeString())
                .append(END_STATEMENT).toString();
        log.debug("Altering column type: " + sql);
        return sql;
    }
}
