package org.avniproject.etl.repository;

import org.avniproject.etl.BaseIntegrationTest;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.result.SyncRegistrationConcept;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AvniMetadataRepositoryTest extends BaseIntegrationTest {

    @Autowired
    private AvniMetadataRepository avniMetadataRepository;

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetConceptName() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc"));
        String conceptName = avniMetadataRepository.conceptName("f005ccf7-f714-4615-a2a0-26efa2da6491");
        assertThat(conceptName, is("Numeric Question"));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldLookForConceptNameWithinOrganisation() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("orgb", "orgb", "orgb"));
        assertThrows(Exception.class, () -> avniMetadataRepository.conceptName("f005ccf7-f714-4615-a2a0-26efa2da6491"));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldRetrieveSyncRegistrationConcepts() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc"));
        SyncRegistrationConcept[] syncRegistrationConcepts = avniMetadataRepository.findSyncRegistrationConcepts("a95d8951-17e4-408d-98b0-ef3a6c982b96");
        SyncRegistrationConcept concept1 = syncRegistrationConcepts[0];
        assertThat(concept1.getUuid(), is("701b68df-dc52-4d69-ab91-f03a70ac1bbc"));
        assertThat(concept1.getName(), is("Text Question"));
        SyncRegistrationConcept concept2 = syncRegistrationConcepts[1];
        assertThat(concept2.getUuid(), is(nullValue()));
        assertThat(concept2.getName(), is(nullValue()));
    }
}
