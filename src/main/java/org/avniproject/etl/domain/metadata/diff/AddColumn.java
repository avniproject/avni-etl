package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.avniproject.etl.domain.metadata.diff.Strings.*;

public class AddColumn implements Diff {
    private final String tableName;
    private final Column column;
    private static final Logger log = LoggerFactory.getLogger(AddColumn.class);

    public AddColumn(String tableName, Column column) {
        this.tableName = tableName;
        this.column = column;
    }

    @Override
    public String getSql() {
        String alter_table_add_column = new StringBuffer()
                .append("alter table ")
                .append(QUOTE)
                .append(OrgIdentityContextHolder.getDbSchema())
                .append(QUOTE)
                .append(DOT)
                .append(tableName)
                .append(" add column ")
                .append(QUOTE)
                .append(column.getName())
                .append(QUOTE)
                .append(SPACE)
                .append(column.getType().typeString())
                .append(END_STATEMENT).toString();
        log.debug("Altering table to add column:" +  alter_table_add_column);
        return alter_table_add_column;
    }
}
