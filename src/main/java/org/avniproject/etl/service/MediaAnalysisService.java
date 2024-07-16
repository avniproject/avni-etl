package org.avniproject.etl.service;

import org.apache.log4j.Logger;
import org.avniproject.etl.config.AmazonClientService;
import org.avniproject.etl.config.EtlServiceConfig;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.repository.OrganisationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MediaAnalysisService {
    private final OrganisationRepository organisationRepository;
    private final OrganisationFactory organisationFactory;
    private final SchemaMigrationService schemaMigrationService;
    private final SyncService syncService;
    private final EtlServiceConfig etlServiceConfig;
    private final AmazonClientService amazonClientService;
    private static final Logger log = Logger.getLogger(MediaAnalysisService.class);

    @Autowired
    public MediaAnalysisService(OrganisationRepository organisationRepository, OrganisationFactory organisationFactory, SchemaMigrationService schemaMigrationService, SyncService syncService, EtlServiceConfig etlServiceConfig, AmazonClientService amazonClientService) {
        this.organisationRepository = organisationRepository;
        this.organisationFactory = organisationFactory;
        this.schemaMigrationService = schemaMigrationService;
        this.syncService = syncService;
        this.etlServiceConfig = etlServiceConfig;
        this.amazonClientService = amazonClientService;
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
//        TODO
        ArrayList<String> listOfAllMediaUrls = amazonClientService.listObjectsInBucket(getMediaDirectory(organisation), "thumbnails");
        ArrayList<String> listOfAllMediaUrlsIncludingThumbnails = amazonClientService.listObjectsInBucket(getMediaDirectory(organisation), "");
        ArrayList<String> listOfAllThumbnailsUrls = amazonClientService.listObjectsInBucket(getThumbnailsDirectory(organisation), "");
        log.info(String.format("listOfAllMediaUrls %d listOfAllMediaUrlsIncludingThumbnails %d listOfAllThumbnailsUrls %d", listOfAllMediaUrls.size(), listOfAllMediaUrlsIncludingThumbnails.size(), listOfAllThumbnailsUrls.size()));
        log.info(String.format("Completed Media Analysis for schema %s with dbUser %s and schemaUser %s", organisationIdentity.getSchemaName(), organisationIdentity.getDbUser(), organisationIdentity.getSchemaUser()));
        OrgIdentityContextHolder.setContext(organisationIdentity, etlServiceConfig);
    }

    private String getThumbnailsDirectory(Organisation organisation) {
        return getMediaDirectory(organisation) + "/thumbnails";
    }

    private String getMediaDirectory(Organisation organisation) {
        return organisation.getOrganisationIdentity().getMediaDirectory();
    }
}
