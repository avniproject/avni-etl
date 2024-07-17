package org.avniproject.etl.service;

import org.apache.log4j.Logger;
import org.avniproject.etl.config.AmazonClientService;
import org.avniproject.etl.config.EtlServiceConfig;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.repository.OrganisationRepository;
import org.glassfish.jaxb.core.v2.TODO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.swing.text.html.HTML;
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
        ArrayList<String> listOfAllMediaUrls = amazonClientService.listObjectsInBucket(getMediaDirectory(organisation));
        //TODO Fix test issues causing build break
        //TODO Make use of listOfAllMediaUrls to come up with required subset of URLs, like thumbnails, media after validating UUID and excluding Mobile and Adhoc entries
        //TODO Log entries that get filtered out for dev purposes
        ArrayList<String> listOfAllMediaUrlsExcludingThumbnails = new ArrayList<>();
        ArrayList<String> listOfAllThumbnailsUrls = new ArrayList<>();
        // TODO: 17/07/24 Fetch list of MediaUrls from media table
        //        SELECT REPLACE(image_url, 'https://s3.ap-south-1.amazonaws.com/prod-user-media/goonj/', '') as image_url_in_media_table
        //        FROM goonj.media
        //        ORDER BY REPLACE(image_url, 'https://s3.ap-south-1.amazonaws.com/prod-user-media/goonj/', '');
        // TODO: 17/07/24 Invoke Analysis method to perform various metrics computations for each entry in media table of the org
        log.info(String.format("listOfAllMediaUrls %d listOfAllMediaUrlsExcludingThumbnails %d listOfAllThumbnailsUrls %d", listOfAllMediaUrls.size(), listOfAllMediaUrlsExcludingThumbnails.size(), listOfAllThumbnailsUrls.size()));
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
