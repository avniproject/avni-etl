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

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class MediaAnalysisService {
    public static final String THUMBNAILS_PATTERN = "thumbnails";
    public static final String ADHOC_MOBILE_DB_BACKUP_PATTERN = "Adhoc|MobileDbBackup";
    public static final String UUID_V4_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
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

        List<String> listOfAllMediaUrls = fetchValidMediaUrlsFromStorage(organisation);
        Map<Boolean, List<String>> partitionResults = partitionListBasedOnThumbnailsPattern(listOfAllMediaUrls);
        List<String> listOfAllThumbnailsUrls = partitionResults.get(Boolean.TRUE);
        List<String> listOfAllMediaUrlsExcludingThumbnails = partitionResults.get(Boolean.FALSE);
        log.info(String.format("listOfAllMediaUrls %d listOfAllMediaUrlsExcludingThumbnails %d listOfAllThumbnailsUrls %d", listOfAllMediaUrls.size(), listOfAllMediaUrlsExcludingThumbnails.size(), listOfAllThumbnailsUrls.size()));

        //TODO Log entries that get filtered out for dev purposes
        // TODO: 17/07/24 Fetch list of MediaUrls from media table
        //        SELECT REPLACE(image_url, 'https://s3.ap-south-1.amazonaws.com/prod-user-media/goonj/', '') as image_url_in_media_table
        //        FROM goonj.media
        //        ORDER BY REPLACE(image_url, 'https://s3.ap-south-1.amazonaws.com/prod-user-media/goonj/', '');
        // TODO: 17/07/24 Invoke Analysis method to perform various metrics computations for each entry in media table of the org
        //TODO Fix test issues causing build break
        log.info(String.format("Completed Media Analysis for schema %s with dbUser %s and schemaUser %s", organisationIdentity.getSchemaName(), organisationIdentity.getDbUser(), organisationIdentity.getSchemaUser()));
    }

    private List<String> fetchValidMediaUrlsFromStorage(Organisation organisation) {
        List<String> listOfAllMediaUrls = amazonClientService.listObjectsInBucket(getMediaDirectory(organisation));
        filterOutNonMediaUrls(listOfAllMediaUrls);
        return listOfAllMediaUrls;
    }

    private void filterOutNonMediaUrls(List<String> listOfAllMediaUrls) {
        Predicate<String> fastSyncAndAdhocDumpPatternPredicate = Pattern.compile(ADHOC_MOBILE_DB_BACKUP_PATTERN, Pattern.CASE_INSENSITIVE).asPredicate();
        Predicate<String> notUUIDPatternPredicate = Pattern.compile(UUID_V4_PATTERN).asPredicate().negate();
        listOfAllMediaUrls.removeIf(fastSyncAndAdhocDumpPatternPredicate.or(notUUIDPatternPredicate));
    }

    private Map<Boolean, List<String>> partitionListBasedOnThumbnailsPattern(List<String> listOfAllMediaUrls) {
        Predicate<String> thumbnailsPatternPredicate = Pattern.compile(THUMBNAILS_PATTERN, Pattern.CASE_INSENSITIVE).asPredicate();
        Map<Boolean, List<String>> partitionResults= listOfAllMediaUrls.stream().collect(Collectors.partitioningBy(thumbnailsPatternPredicate));
        return partitionResults;
    }

    private String getMediaDirectory(Organisation organisation) {
        return organisation.getOrganisationIdentity().getMediaDirectory();
    }
}
