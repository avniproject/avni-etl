package org.avniproject.etl.service;

import org.avniproject.etl.builder.OrganisationIdentityBuilder;
import org.avniproject.etl.config.StubEtlServiceConfig;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.repository.OrganisationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;

public class EtlServiceTest {
    @Mock
    private OrganisationRepository organisationRepository;

    @Mock
    private OrganisationFactory organisationFactory;

    @Mock
    private SchemaMigrationService schemaMigrationService;

    @Mock
    private ReportingViewService reportingViewService;

    @Mock
    private SyncService syncService;

    private AutoCloseable closeable;

    @BeforeEach
    public void beforeEach() {
        closeable = openMocks(this);
    }

    @AfterEach
    public void afterEach() throws Exception {
        closeable.close();
    }

    @Test
    public void runForOrganisationShouldSetContextForOrganisation() {
        EtlService etlService = new EtlService(organisationRepository, organisationFactory, schemaMigrationService, syncService, new StubEtlServiceConfig(), reportingViewService);
        OrganisationIdentity organisationIdentity = new OrganisationIdentityBuilder().withId(1).withDbUser("a").build();

        Organisation organisation = mock(Organisation.class);
        when(organisation.getSchemaMetadata()).thenReturn(new SchemaMetadata(List.of()));
        when(organisationFactory.create(organisationIdentity)).thenReturn(organisation);
        when(schemaMigrationService.migrate(organisation)).thenReturn(organisation);
        etlService.runFor(organisationIdentity);

        assertThat(OrgIdentityContextHolder.getOrganisationIdentity(), is(organisationIdentity));
    }

    @Test
    public void runForOrganisationShouldCreateOrganisationAndCallEtlServiceForMigration() {
        EtlService etlService = new EtlService(organisationRepository, organisationFactory, schemaMigrationService, syncService, new StubEtlServiceConfig(), reportingViewService);

        OrganisationIdentity organisationIdentity = new OrganisationIdentityBuilder().withId(1).withDbUser("a").build();
        Organisation organisation = mock(Organisation.class);
        when(organisation.getSchemaMetadata()).thenReturn(new SchemaMetadata(List.of()));
        when(organisationFactory.create(organisationIdentity)).thenReturn(organisation);

        Organisation newOrganisation = mock(Organisation.class);
        when(schemaMigrationService.migrate(organisation)).thenReturn(newOrganisation);
        when(newOrganisation.getSchemaMetadata()).thenReturn(new SchemaMetadata(List.of()));
        etlService.runFor(organisationIdentity);

        verify(organisationFactory).create(organisationIdentity);
        verify(schemaMigrationService).migrate(organisation);
        verify(syncService).sync(newOrganisation);
    }
}
