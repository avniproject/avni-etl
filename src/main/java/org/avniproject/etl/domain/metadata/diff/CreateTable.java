package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.avniproject.etl.domain.metadata.diff.Strings.*;

public class CreateTable implements Diff {
    private final List<Column> columns = new ArrayList<>();
    private String name = "";

    public CreateTable(String name, List<Column> columns) {
        this.name = name;
        this.columns.addAll(columns);
    }

    @Override
    public String getSql() {
        StringBuffer sql = new StringBuffer("drop table if exists ")
                .append(this.getTableName())
                .append(" cascade")
                .append(END_STATEMENT)
                .append(NEWLINE)
                .append("create table ")
                .append(this.getTableName())
                .append(OPEN_BRACKETS)
                .append(addColumnsToSql(columns))
                .append(CLOSE_BRACKETS)
                .append(END_STATEMENT)
                .append(NEWLINE);

        List<String> groupDbUsers = OrgIdentityContextHolder.getOrganisationIdentity().getUsersWithSchemaAccess();
        List<StringBuilder> tablePermissions = groupDbUsers.stream()
                .map(user -> grantPermissions(OrgIdentityContextHolder.getDbSchema(), user))
                .collect(Collectors.toList());

        sql.append(String.join("", tablePermissions));

        return sql.toString();
    }

    private StringBuilder grantPermissions(String dbSchema, String user) {
        return new StringBuilder()
                .append(grantPermissionsToObject(dbSchema, user, "tables"))
                .append(grantPermissionsToObject(dbSchema, user, "sequences"));
    }

    private StringBuilder grantPermissionsToObject(String dbSchema, String user, String objectName) {
        return new StringBuilder().append("grant all privileges on all ")
            .append(objectName)
            .append(" in schema ")
            .append(QUOTE)
            .append(dbSchema)
            .append(QUOTE)
            .append(" to ")
            .append(QUOTE)
            .append(user)
            .append(QUOTE)
            .append(END_STATEMENT)
            .append(NEWLINE);
    }

    private String getTableName() {
        return new StringBuilder().append(QUOTE).append(OrgIdentityContextHolder.getDbSchema()).append(QUOTE)
                .append(DOT)
                .append(name)
                .toString();
    }

    private String addColumnsToSql(List<Column> columns) {
        List<String> defaultColumns = columns.stream().map(column ->
                String.valueOf(
                        new StringBuffer()
                                .append(QUOTE)
                                .append(column.getName())
                                .append(QUOTE)
                                .append(SPACE)
                                .append(column.getType().typeString())))
                .collect(Collectors.toList());

        return String.join(COMMA, defaultColumns);
    }
}
