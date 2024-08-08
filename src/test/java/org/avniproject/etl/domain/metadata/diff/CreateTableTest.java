package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.PersonTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class CreateTableTest {

    @BeforeEach
    public void before() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("dbUser", "schema", "mediaDirectory"));
    }

    @Test
    public void shouldCreateSqlWithCommonColumns() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("dbUser", "schema", "mediaDirectory"));
        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setName("tableName");

        assertThat(new CreateTable("tableName", new PersonTable(false).columns()).getSql(), containsString("create table \"schema\".tableName (\"id\" integer, \"uuid\" text, \"is_voided\" boolean, \"created_by_id\" integer, \"last_modified_by_id\" integer, \"created_date_time\" timestamp with time zone, \"last_modified_date_time\" timestamp with time zone, \"organisation_id\" integer, \"address_id\" integer, \"registration_date\" date, \"first_name\" text, \"last_name\" text, \"registration_location\" point, \"legacy_id\" text, \"latest_approval_status\" text, \"date_of_birth\" date, \"date_of_birth_verified\" boolean, \"gender\" text );\ngrant all privileges on all tables in schema \"schema\" to \"dbUser\";"));
    }
}
