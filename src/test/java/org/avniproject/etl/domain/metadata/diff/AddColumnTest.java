package org.avniproject.etl.domain.metadata.diff;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.Column;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class AddColumnTest {
    @Test
    public void shouldAddColumn() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("dbUser", "schema", "mediaDirectory"));
        AddColumn addColumn = new AddColumn("table", new Column("name", Column.Type.text));
        assertThat(addColumn.getSql(), is("""
alter table "schema".table add column "name" text;"""));
    }

    @Test
    public void checkNameLengthBeforeShortening() {
        String columnName = "Total silt requested by the family members – Number of trolleys";
        String shortenedColumnName = "Total silt requested by the family members – Nu (1206887472)";
        Column longColumn = new Column(columnName, Column.Type.numeric);
        assertEquals(shortenedColumnName, longColumn.getName());
    }

    @Test
    public void shouldAddColumnWithLargeNameAfterShortening() {
        String columnName = "Total silt requested by the family members – Number of trolleys";
        String shortenedColumnName = "Total silt requested by the family members – Nu (1206887472)";
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("dbUser", "schema", "mediaDirectory"));
        AddColumn addColumn = new AddColumn("table", new Column(columnName, Column.Type.text));
        assertThat(addColumn.getSql(), is("""
alter table "schema".table add column "%s" text;""".formatted(shortenedColumnName)));
    }
}
