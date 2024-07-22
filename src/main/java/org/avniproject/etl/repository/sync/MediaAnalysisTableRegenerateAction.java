package org.avniproject.etl.repository.sync;

import org.apache.log4j.Logger;
import org.avniproject.etl.config.AmazonClientService;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.dto.MediaAnalysisVO;
import org.avniproject.etl.dto.MediaDTO;
import org.avniproject.etl.repository.MediaTableRepository;
import org.avniproject.etl.repository.sql.SqlFile;
import org.avniproject.etl.service.MediaAnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Repository
public class MediaAnalysisTableRegenerateAction {
    public static final String THUMBNAILS_PATTERN = "thumbnails";
    public static final String ADHOC_MOBILE_DB_BACKUP_PATTERN = "Adhoc|MobileDbBackup";
    public static final String UUID_V4_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";

    private final AmazonClientService amazonClientService;
    private final MediaTableRepository mediaTableRepository;
    private final JdbcTemplate jdbcTemplate;
    private static final String generateMediaAnalysisTableTemplate = SqlFile.readSqlFile("mediaAnalysis.sql.st");

    private static final Logger log = Logger.getLogger(MediaAnalysisService.class);

    @Autowired
    public MediaAnalysisTableRegenerateAction(AmazonClientService amazonClientService, MediaTableRepository mediaTableRepository, JdbcTemplate jdbcTemplate) {
        this.amazonClientService = amazonClientService;
        this.mediaTableRepository = mediaTableRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void process(Organisation organisation, TableMetadata tableMetadata) {

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
        List<MediaDTO> listOfMediaEntities = mediaTableRepository.getAllMedia();
        String orgMediaDirectory = organisation.getOrganisationIdentity().getMediaDirectory();
        // TODO: 22/07/24 do  
        List<MediaAnalysisVO> mediaAnalysisVOS = listOfMediaEntities.stream().map(mediaDTO -> {
            boolean isValidUrl = mediaDTO.url().contains(orgMediaDirectory);
            String urlToSearch = mediaDTO.url().substring(mediaDTO.url().indexOf(orgMediaDirectory));
            boolean isPresentInStorage = listOfAllMediaUrlsExcludingThumbnails.contains(urlToSearch);
            // TODO: 22/07/24 init booleans correctly
            return new MediaAnalysisVO(mediaDTO.uuid(), mediaDTO.url(), isValidUrl, isPresentInStorage, false);
        }).collect(Collectors.toList());
        log.info(String.format("listOfMediaEntities %d mediaAnalysisVOS %d ", listOfMediaEntities.size(), mediaAnalysisVOS.size()));

        truncateMediaAnalysisTable(tableMetadata);
        generateMediaAnalysisTableEntries(tableMetadata, mediaAnalysisVOS);
    }

    private void truncateMediaAnalysisTable(TableMetadata tableMetadata) {
        String schema = OrgIdentityContextHolder.getDbSchema();
        String mediaAnalysisTable = tableMetadata.getName();
        String sql = new ST("delete from <schemaName>.<mediaAnalysisTable> where uuid is not null;")
                .add("schemaName", wrapInQuotes(schema))
                .add("mediaAnalysisTable", wrapInQuotes(mediaAnalysisTable))
                .render();
        runInOrgContext(() -> {
            jdbcTemplate.execute(sql);
            return NullObject.instance();
        }, jdbcTemplate);
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

    private void generateMediaAnalysisTableEntries(TableMetadata tableMetadata, List<MediaAnalysisVO> mediaAnalysisVOS) {
        String schema = OrgIdentityContextHolder.getDbSchema();
        String mediaAnalysisTable = tableMetadata.getName();
        String sql = new ST(generateMediaAnalysisTableTemplate)
                .add("schemaName", wrapInQuotes(schema))
                .add("mediaAnalysisTable", wrapInQuotes(mediaAnalysisTable))
                .render();
        runInOrgContext(() -> {
            jdbcTemplate.batchUpdate(sql,
                    mediaAnalysisVOS,
                    100,
                    (ps, mediaAnalysisVO) -> {
                        ps.setString(1, mediaAnalysisVO.getUuid());
                        ps.setString(2, mediaAnalysisVO.getImage_url());
                        ps.setBoolean(3, mediaAnalysisVO.isValidUrl());
                        ps.setBoolean(4, mediaAnalysisVO.isPresentInStorage());
                        ps.setBoolean(5, mediaAnalysisVO.isThumbnailGenerated());
                    });
            return NullObject.instance();
        }, jdbcTemplate);
    }

    private String wrapInQuotes(String parameter) {
        return parameter == null ? "null" : "\"" + parameter + "\"";
    }

}
