package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.OrganisationIdentity;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class OrganisationIdentityRowMapper implements RowMapper<OrganisationIdentity> {
    @Override
    public OrganisationIdentity mapRow(ResultSet rs, int rowNum) throws SQLException {
        String dbUser = rs.getString("db_user");
        return OrganisationIdentity.createForOrganisation(dbUser, rs.getString("schema_name"), rs.getString("media_directory"));
    }
}
