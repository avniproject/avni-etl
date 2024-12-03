package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.builder.OrganisationIdentityBuilder;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;


class IndexMetadataTest {

    @BeforeEach
    public void setup() {
        OrgIdentityContextHolder.setContext(new OrganisationIdentityBuilder().build());
    }

    @Test
    public void matches_shouldMatchColumnMetadata() {
        ColumnMetadata id = new ColumnMetadata(new Column("id", Column.Type.numeric), null, null, null, false);
        ColumnMetadata uuid = new ColumnMetadata(new Column("uuid", Column.Type.numeric), null, null, null, false);

        assertThat(id.matches(id), is(true));
        assertThat(id.matches(uuid), is(false));

        assertThat(new IndexMetadata(id).matches(new IndexMetadata(1, "orgc_as123_idx", id)), is(true));
        assertThat(new IndexMetadata(id).matches(new IndexMetadata(1, "orgc_as123_idx", uuid)), is(false));
        assertThat(new IndexMetadata(1, "orgc_as123_idx", uuid).matches(new IndexMetadata(id)), is(false));
    }

}
