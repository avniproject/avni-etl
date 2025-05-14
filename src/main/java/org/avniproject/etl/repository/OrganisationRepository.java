package org.avniproject.etl.repository;

import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.User;
import org.avniproject.etl.repository.rowMappers.OrganisationGroupRowMapper;
import org.avniproject.etl.repository.rowMappers.OrganisationIdentityRowMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class OrganisationRepository {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public OrganisationRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public OrganisationIdentity getOrganisation(String organisationUUID) {
        String query = "select db_user, schema_name, media_directory\n" +
                "from organisation where uuid = ? and is_voided = false";
        List<OrganisationIdentity> organisationIdentities = jdbcTemplate.query(query, ps -> ps.setString(1, organisationUUID), new OrganisationIdentityRowMapper());
        if (organisationIdentities.size() == 0) return null;
        return organisationIdentities.get(0);
    }

    public List<OrganisationIdentity> getOrganisationGroup(String organisationGroupUUID) {
        String query = "select o.db_user db_user, og.schema_name schema_name, og.db_user schema_db_user\n" +
                "from organisation_group og\n" +
                "         inner join organisation_group_organisation ogo on og.id = ogo.organisation_group_id\n" +
                "         inner join organisation o on ogo.organisation_id = o.id\n" +
                "where og.uuid = ?";
        List<OrganisationIdentity> organisationIdentities = jdbcTemplate.query(query, ps -> ps.setString(1, organisationGroupUUID), new OrganisationGroupRowMapper());

        String orgGroupOrgDbUsersQuery = "select o.db_user db_user\n" +
                "from organisation_group og\n" +
                "         inner join organisation_group_organisation ogo on og.id = ogo.organisation_group_id\n" +
                "         inner join organisation o on ogo.organisation_id = o.id\n" +
                "where og.uuid = ?";
        List<String> orgGroupOrgDbUsers = jdbcTemplate.query(orgGroupOrgDbUsersQuery, ps -> ps.setString(1, organisationGroupUUID), (rs, rowNum) -> rs.getString(1));
        organisationIdentities.forEach(organisationIdentity -> organisationIdentity.setOrgGroupOrgDbUsers(orgGroupOrgDbUsers));
        return organisationIdentities;
    }

    public void createDBUser(String name, String pass) {
        jdbcTemplate.queryForMap("select create_db_user(?, ?)", name, pass);
    }

    public void createImplementationSchema(String schemaName, String dbUser) {
        jdbcTemplate.queryForMap("select create_implementation_schema(?, ?)", schemaName, dbUser);
    }

    public OrganisationIdentity getOrganisationByUser(User user) {
        List<String> organisationUUIDs = jdbcTemplate.query("select o.uuid from users \n" +
                "    join organisation o on users.organisation_id = o.id\n" +
                "where users.organisation_id = o.id and users.uuid = ?", ps -> ps.setString(1, user.getUuid()), (rs, rowNum) -> rs.getString(1));
        if (organisationUUIDs.size() == 0) return null;
        return this.getOrganisation(organisationUUIDs.get(0));
    }

    public List<Long> getOrganisationIds(OrganisationIdentity organisationIdentity) {
        if (organisationIdentity.isPartOfGroup()) {
            return jdbcTemplate.query("select o.id from organisation o\n" +
                    "join organisation_group_organisation ogo on o.id = ogo.organisation_id\n" +
                    "join organisation_group og on ogo.organisation_group_id = og.id\n" +
                    "where og.schema_name = ?",
                    ps -> ps.setString(1, organisationIdentity.getSchemaName()),
                    (rs, rowNum) -> rs.getLong(1));
        } else {
            String query = "select o.id from organisation o\n" +
                    "where o.schema_name = ?";
            return jdbcTemplate.query(query,
                    ps -> ps.setString(1, organisationIdentity.getSchemaName()),
                    (rs, rowNum) -> rs.getLong(1));
        }
    }
}
