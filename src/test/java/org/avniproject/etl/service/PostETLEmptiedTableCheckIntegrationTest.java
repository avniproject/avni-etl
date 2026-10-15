package org.avniproject.etl.service;

import org.avniproject.etl.BaseIntegrationTest;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.NestedExceptionUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PostETLEmptiedTableCheckIntegrationTest extends BaseIntegrationTest {
    private static final String SCHEMA = "postetl_check_test";

    @Autowired
    private PostETLSyncService postETLSyncService;
    private Organisation organisation;

    @BeforeEach
    public void beforeEach() {
        jdbcTemplate.execute("drop schema if exists " + SCHEMA + " cascade");
        jdbcTemplate.execute("create schema " + SCHEMA);
        jdbcTemplate.execute("create table " + SCHEMA + ".src (id int)");
        OrganisationIdentity identity = OrganisationIdentity.createForOrganisation("orga", SCHEMA, "orga");
        OrgIdentityContextHolder.setContext(identity);
        organisation = new Organisation(identity, null, null);
    }

    @AfterEach
    public void afterEach() {
        jdbcTemplate.execute("drop schema if exists " + SCHEMA + " cascade");
        jdbcTemplate.execute("delete from public.post_etl_sync_status where db_user = 'orga'");
        OrgIdentityContextHolder.clear();
    }

    @Test
    public void firstRunWithNothingToInsertPasses() {
        postETLSyncService.executePostETLScripts(organisation);

        assertThat(countOfRowsIn(SCHEMA + ".t"), equalTo(0L));
    }

    @Test
    public void refreshThatEmptiesATableWithRowsFailsAndKeepsTheRows() {
        postETLSyncService.executePostETLScripts(organisation);
        jdbcTemplate.execute("insert into " + SCHEMA + ".t values (1), (2)");
        jdbcTemplate.execute("analyze " + SCHEMA + ".t");

        RuntimeException failure = assertThrows(RuntimeException.class, () -> postETLSyncService.executePostETLScripts(organisation));

        assertThat(NestedExceptionUtils.getMostSpecificCause(failure).getMessage(),
                containsString("postetl_check_test.t had rows before this refresh and is empty after it"));
        assertThat(countOfRowsIn(SCHEMA + ".t"), equalTo(2L));
    }

    @Test
    public void refreshThatKeepsRowsPasses() {
        postETLSyncService.executePostETLScripts(organisation);
        jdbcTemplate.execute("insert into " + SCHEMA + ".t values (1), (2)");
        jdbcTemplate.execute("analyze " + SCHEMA + ".t");
        jdbcTemplate.execute("insert into " + SCHEMA + ".src values (3)");

        postETLSyncService.executePostETLScripts(organisation);

        assertThat(countOfRowsIn(SCHEMA + ".t"), equalTo(1L));
    }
}
