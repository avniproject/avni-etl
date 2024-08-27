package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;

import static org.avniproject.etl.domain.metadata.diff.Strings.*;

public class AddIndex implements Diff {
    private final String name;
    private final String tableName;
    private final String columnName;

    public AddIndex(String name, String tableName, String columnName) {
        this.name = name;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public String getSql() {
        return new StringBuffer()
                .append("create index ")
                .append(QUOTE)
                .append(name)
                .append(QUOTE)
                .append(" on ")
                .append(QUOTE)
                .append(OrgIdentityContextHolder.getDbSchema())
                .append(QUOTE)
                .append(DOT)
                .append(QUOTE)
                .append(tableName)
                .append(QUOTE)
                .append(OPEN_BRACKETS)
                .append(QUOTE)
                .append(columnName)
                .append(QUOTE)
                .append(CLOSE_BRACKETS)
                .append(END_STATEMENT)
                .toString();
    }
}
