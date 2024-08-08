package org.avniproject.etl.repository.sync;

import org.apache.log4j.Logger;
import org.avniproject.etl.config.AmazonClientService;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.dto.MediaAnalysisVO;
import org.avniproject.etl.dto.MediaCompactDTO;
import org.avniproject.etl.repository.MediaTableRepository;
import org.avniproject.etl.repository.sql.SqlFile;
import org.avniproject.etl.service.MediaAnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Repository
public class MediaAnalysisTableRegenerateAction {
    public static final String THUMBNAILS_PATTERN = "thumbnails";
    public static final String ADHOC_MOBILE_DB_BACKUP_PATTERN = "Adhoc|MobileDbBackup";
    public static final String UUID_V4_PATTERN = "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
    public static final String STRING_CONST_SEPARATOR = "/";
    public static final String TRUNCATE_MEDIA_ANALYSIS_TABLE_SQL = "delete from <schemaName>.<mediaAnalysisTable> where uuid is not null;";
    public static final String SCHEMA_NAME = "schemaName";
    public static final String MEDIA_ANALYSIS_TABLE = "mediaAnalysisTable";
    public static final int INT_CONSTANT_ZERO = 0;
    public static final int INT_CONSTANT_ONE = 1;

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
        Map<Boolean, Map<String, String>> partitionResults = partitionListBasedOnThumbnailsPattern(listOfAllMediaUrls);
        Map<String, String> thumbnailUrlsMap = partitionResults.get(Boolean.TRUE);
        Map<String, String> mediaUrlsMap = partitionResults.get(Boolean.FALSE);

        String orgMediaDirectory = organisation.getOrganisationIdentity().getMediaDirectory();
        List<MediaCompactDTO> listOfMediaDTOEntities = mediaTableRepository.getAllMedia();

        Map<String, List<MediaCompactDTO>> groupedMediaEntityMap = listOfMediaDTOEntities.stream()
                .collect(Collectors.groupingBy(mediaDTO -> mediaDTO.compositeUUID()));
        List<MediaAnalysisVO> mediaAnalysisVOS = groupedMediaEntityMap.entrySet().stream().map(groupedMediaEntityMapEntry -> {
            MediaCompactDTO mediaDTO = groupedMediaEntityMapEntry.getValue().get(INT_CONSTANT_ZERO);
            boolean isPresentInStorage = false, isThumbnailGenerated = false;
            boolean isValidUrl = mediaDTO.url().contains(orgMediaDirectory);
            if (isValidUrl) {
                String urlToSearch = mediaDTO.url().substring(mediaDTO.url().lastIndexOf(STRING_CONST_SEPARATOR));
                isPresentInStorage = mediaUrlsMap.containsKey(urlToSearch);
                isThumbnailGenerated = thumbnailUrlsMap.containsKey(urlToSearch);
            }
            return new MediaAnalysisVO(mediaDTO.entityUUID(),
                    mediaDTO.url(), isValidUrl, isPresentInStorage, isThumbnailGenerated,
                    groupedMediaEntityMapEntry.getValue().size() > INT_CONSTANT_ONE);
        }).collect(Collectors.toList());
        log.info(String.format("listOfMediaDTOEntities %d mediaAnalysisVOS %d duplicates %d", listOfMediaDTOEntities.size(), mediaAnalysisVOS.size(), listOfMediaDTOEntities.size() - mediaAnalysisVOS.size()));

        truncateMediaAnalysisTable(tableMetadata);
        generateMediaAnalysisTableEntries(tableMetadata, mediaAnalysisVOS);
    }

    private void truncateMediaAnalysisTable(TableMetadata tableMetadata) {
        String schema = OrgIdentityContextHolder.getDbSchema();
        String mediaAnalysisTable = tableMetadata.getName();
        String sql = new ST(TRUNCATE_MEDIA_ANALYSIS_TABLE_SQL)
                .add(SCHEMA_NAME, wrapInQuotes(schema))
                .add(MEDIA_ANALYSIS_TABLE, wrapInQuotes(mediaAnalysisTable))
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

    private Map<Boolean, Map<String, String>> partitionListBasedOnThumbnailsPattern(List<String> listOfAllMediaUrls) {
        Predicate<String> thumbnailsPatternPredicate = Pattern.compile(THUMBNAILS_PATTERN, Pattern.CASE_INSENSITIVE).asPredicate();
        Map<Boolean, Map<String, String>> partitionResults= listOfAllMediaUrls.stream().collect(Collectors.partitioningBy(thumbnailsPatternPredicate,
                Collectors.toMap(url -> url.substring(url.lastIndexOf(STRING_CONST_SEPARATOR)), Function.identity())));
        return partitionResults;
    }

    private String getMediaDirectory(Organisation organisation) {
        return organisation.getOrganisationIdentity().getMediaDirectory();
    }

    private void generateMediaAnalysisTableEntries(TableMetadata tableMetadata, List<MediaAnalysisVO> mediaAnalysisVOS) {
        String schema = OrgIdentityContextHolder.getDbSchema();
        String mediaAnalysisTable = tableMetadata.getName();
        String sql = new ST(generateMediaAnalysisTableTemplate)
                .add(SCHEMA_NAME, wrapInQuotes(schema))
                .add(MEDIA_ANALYSIS_TABLE, wrapInQuotes(mediaAnalysisTable))
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
                        ps.setBoolean(6, mediaAnalysisVO.isHavingDuplicates());
                    });
            return NullObject.instance();
        }, jdbcTemplate);
    }

    private String wrapInQuotes(String parameter) {
        return parameter == null ? "null" : "\"" + parameter + "\"";
    }

}
