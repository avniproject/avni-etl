package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.Column;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AlterColumnTypeTest {
    @Test
    public void shouldGenerateAlterColumnTypeSql() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("dbUser", "schema", "mediaDirectory"));
        AlterColumnType alterColumnType = new AlterColumnType("media", "address_id", Column.Type.integer);
        assertThat(alterColumnType.getSql(), is("""
alter table "schema".media alter column "address_id" type integer using "address_id"::integer;"""));
    }
}
