package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.UserTable;

import java.util.stream.Collectors;

public class UserTableMetadataBuilder {
    public static TableMetadata build() {
        TableMetadata userTableMetadata = new TableMetadata();
        UserTable userTable = new UserTable();
        userTableMetadata.setName(userTable.name(null));
        userTableMetadata.setType(TableMetadata.Type.User);
        userTableMetadata.addColumnMetadata(userTable.columns().stream().map(column -> new ColumnMetadata(column, null, null, null, false)).collect(Collectors.toList()));

        return userTableMetadata;
    }
}
