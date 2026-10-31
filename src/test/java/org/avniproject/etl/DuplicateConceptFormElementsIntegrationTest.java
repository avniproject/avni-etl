package org.avniproject.etl;

import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.service.EtlService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * #173: a form with two live form elements on the same concept must sync, with one column for the concept.
 */
public class DuplicateConceptFormElementsIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private EtlService etlService;

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/duplicate-concept-form-elements.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void twoFormElementsOnOneConceptSyncWithASingleColumn() {
        etlService.runFor(OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc"));

        Long columns = jdbcTemplate.queryForObject(
                "select count(*) from information_schema.columns where table_schema = 'orgc' and table_name = 'person' and column_name = 'Phone Number'", Long.class);
        assertThat(columns, equalTo(1L));
        List<Map<String, Object>> persons = jdbcTemplate.queryForList("select * from orgc.person");
        assertThat((long) persons.size(), greaterThan(0L));
        assertThat(persons.get(0).get("Phone Number"), equalTo(new BigDecimal("9876543210")));
    }
}
