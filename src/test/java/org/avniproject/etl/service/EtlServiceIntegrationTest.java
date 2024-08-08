package org.avniproject.etl.service;

import org.avniproject.etl.BaseIntegrationTest;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;

import java.util.TimeZone;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EtlServiceIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private EtlService etlService;

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldNotFailWhenRunTwice() {

        etlService.runFor(OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc"));
        etlService.runFor(OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc"));

        assertThat(countOfRowsIn("orgc.goat"), equalTo(1L));
        assertThat(countOfRowsIn("orgc.household"), equalTo(1L));
        assertThat(countOfRowsIn("orgc.person"), equalTo(2L));
        assertThat(countOfRowsIn("orgc.person_nutrition"), equalTo(3L));
        assertThat(countOfRowsIn("orgc.person_nutrition_exit"), equalTo(2L));
        assertThat(countOfRowsIn("orgc.person_nutrition_growth_monitoring"), equalTo(3L));
        assertThat(countOfRowsIn("orgc.person_nutrition_growth_monitoring_cancel"), equalTo(1L));
        assertThat(countOfRowsIn("orgc.sync_telemetry"), equalTo(1L));
    }
}
