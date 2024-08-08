package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RenameTableTest {

    @Test
    public void shouldRenameTable() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("dbUser", "schema", "mediaDirectory"));
        RenameTable renameTable = new RenameTable("old_name", "new_name");
        assertThat(renameTable.getSql(), is("alter table \"schema\".old_name rename to new_name;"));
    }
}
