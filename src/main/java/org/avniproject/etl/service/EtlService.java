package org.avniproject.etl.service;

import org.apache.log4j.Logger;
import org.avniproject.etl.config.EtlServiceConfig;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.repository.OrganisationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class EtlService {
    private final OrganisationRepository organisationRepository;
    private final OrganisationFactory organisationFactory;
    private final SchemaMigrationService schemaMigrationService;
    private final SyncService syncService;
    private final EtlServiceConfig etlServiceConfig;
    private final ReportingViewService reportingViewService;
    private final PostETLSyncService postETLSyncService;
    private static final Logger log = Logger.getLogger(EtlService.class);

    @Autowired
    public EtlService(OrganisationRepository organisationRepository, OrganisationFactory organisationFactory,
                      SchemaMigrationService schemaMigrationService, SyncService syncService, EtlServiceConfig etlServiceConfig, 
                      ReportingViewService reportingViewService, PostETLSyncService postETLSyncService) {
        this.organisationRepository = organisationRepository;
        this.organisationFactory = organisationFactory;
        this.schemaMigrationService = schemaMigrationService;
        this.syncService = syncService;
        this.etlServiceConfig = etlServiceConfig;
        this.reportingViewService = reportingViewService;
        this.postETLSyncService = postETLSyncService;
    }

    public void runFor(String organisationUUID) {
        OrganisationIdentity organisationIdentity = organisationRepository.getOrganisation(organisationUUID);
        this.runFor(organisationIdentity);
    }

    public void runForOrganisationGroup(String organisationGroupUUID) {
        List<OrganisationIdentity> organisationIdentities = organisationRepository.getOrganisationGroup(organisationGroupUUID);
        this.runFor(organisationIdentities);
    }

    public void runFor(List<OrganisationIdentity> organisationIdentities) {
        organisationIdentities.forEach(this::runFor);
    }

    public void runFor(OrganisationIdentity organisationIdentity) {
        log.info(String.format("Running ETL for %s", organisationIdentity.toString()));
            OrgIdentityContextHolder.setContext(organisationIdentity, etlServiceConfig);
            Organisation organisation = organisationFactory.create(organisationIdentity);
            log.info(String.format("Old organisation schema summary %s", organisation.getSchemaMetadata().getCountsByType()));
            Organisation newOrganisation = schemaMigrationService.migrate(organisation);
            log.info(String.format("New organisation after migration, schema summary %s", newOrganisation.getSchemaMetadata().getCountsByType()));
            reportingViewService.processMetabaseViews(newOrganisation);
            syncService.sync(newOrganisation);
            postETLSyncService.executePostETLScripts(newOrganisation);
            log.info(String.format("Completed ETL for %s", organisationIdentity.toString()));
            OrgIdentityContextHolder.setContext(organisationIdentity, etlServiceConfig);
    }
}
