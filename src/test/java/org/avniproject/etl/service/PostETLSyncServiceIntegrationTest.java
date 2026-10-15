package org.avniproject.etl.service;

import org.avniproject.etl.BaseIntegrationTest;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

// One pooled connection, so the query after the post-ETL run borrows the connection the run used
@TestPropertySource(properties = "spring.datasource.hikari.maximum-pool-size=1")
public class PostETLSyncServiceIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private PostETLSyncService postETLSyncService;

    @AfterEach
    public void afterEach() {
        jdbcTemplate.execute("delete from public.post_etl_sync_status where db_user = 'orga'");
        OrgIdentityContextHolder.clear();
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void roleSetByADdlScriptDoesNotOutliveThePostEtlRun() {
        String loginUser = jdbcTemplate.queryForObject("select session_user", String.class);
        OrganisationIdentity identity = OrganisationIdentity.createForOrganisation("orga", "postetl_role_test", "orga");
        OrgIdentityContextHolder.setContext(identity);

        postETLSyncService.executePostETLScripts(new Organisation(identity, null, null));

        assertThat(jdbcTemplate.queryForObject("select current_user", String.class), equalTo(loginUser));
    }
}
