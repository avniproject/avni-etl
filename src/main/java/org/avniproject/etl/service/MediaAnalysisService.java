package org.avniproject.etl.service;

import org.apache.log4j.Logger;
import org.avniproject.etl.config.EtlServiceConfig;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.OrganisationRepository;
import org.avniproject.etl.repository.sync.MediaAnalysisTableRegenerateAction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class MediaAnalysisService {
    private final MediaAnalysisTableRegenerateAction mediaAnalysisTableRegenerateAction;
    private final OrganisationRepository organisationRepository;
    private final OrganisationFactory organisationFactory;
    private final EtlServiceConfig etlServiceConfig;
    private static final Logger log = Logger.getLogger(MediaAnalysisService.class);

    @Autowired
    public MediaAnalysisService(MediaAnalysisTableRegenerateAction mediaAnalysisTableRegenerateAction, OrganisationRepository organisationRepository, OrganisationFactory organisationFactory,
                                EtlServiceConfig etlServiceConfig) {
        this.mediaAnalysisTableRegenerateAction = mediaAnalysisTableRegenerateAction;
        this.organisationRepository = organisationRepository;
        this.organisationFactory = organisationFactory;
        this.etlServiceConfig = etlServiceConfig;
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
        log.info(String.format("Running Media Analysis for %s", organisationIdentity.toString()));
        OrgIdentityContextHolder.setContext(organisationIdentity, etlServiceConfig);
        Organisation organisation = organisationFactory.create(organisationIdentity);
        Optional<TableMetadata> mediaAnalysisTableMetadata = organisation.getSchemaMetadata().getMediaAnalysisTable();
        if(!mediaAnalysisTableMetadata.isPresent()) {
            log.error(String.format("Sync job hasn't yet run for schema %s with dbUser %s and schemaUser %s", organisationIdentity.getSchemaName(), organisationIdentity.getDbUser(), organisationIdentity.getSchemaUser()));
            return;
        }
        mediaAnalysisTableRegenerateAction.process(organisation,mediaAnalysisTableMetadata.get());
        log.info(String.format("Completed Media Analysis for %s", organisationIdentity.toString()));
    }
}