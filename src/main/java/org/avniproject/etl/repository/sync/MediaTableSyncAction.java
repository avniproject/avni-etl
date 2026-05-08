package org.avniproject.etl.repository.sync;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.result.SyncRegistrationConcept;
import org.avniproject.etl.repository.AvniMetadataRepository;
import org.avniproject.etl.repository.rowMappers.TableNameGenerator;
import org.avniproject.etl.service.MediaService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.avniproject.etl.domain.metadata.ColumnMetadata.MEDIA_COLUMN_CONCEPT_TYPES;
import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;
import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

@Repository
public class MediaTableSyncAction implements EntitySyncAction {
    private static final Logger logger = Logger.getLogger(MediaTableSyncAction.class.getName());
    private final JdbcTemplate jdbcTemplate;
    private final AvniMetadataRepository avniMetadataRepository;
    private static final String mediaSql = readSqlFile("media.sql.st");
    private static final String mediaV3Sql = readSqlFile("mediaV3.sql.st");
    private static final String deleteDuplicateMediaSql = readSqlFile("deleteDuplicateMedia.sql.st");

    private final MediaService mediaService;

    @Autowired
    public MediaTableSyncAction(JdbcTemplate jdbcTemplate, AvniMetadataRepository metadataRepository, MediaService mediaService) {
        this.jdbcTemplate = jdbcTemplate;
        this.avniMetadataRepository = metadataRepository;
        this.mediaService = mediaService;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !tableMetadata.getType().equals(TableMetadata.Type.Media);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        if (this.doesntSupport(tableMetadata)) {
            return;
        }

        currentSchemaMetadata.getTableMetadata().forEach(thisTableMetadata -> {
            // Skip RepeatableQuestionGroup tables - they're handled by MediaForRepeatableGroupsSyncAction
            if (thisTableMetadata.isRepeatableQuestionGroupTable()) {
                return;
            }
            
            List<ColumnMetadata> allMediaColumns = thisTableMetadata.findColumnsMatchingConceptType(MEDIA_COLUMN_CONCEPT_TYPES);
            allMediaColumns.forEach(mediaColumn -> {
                int version = 3; //default for rest
                // Legacy Image columns use the original SQL format
                if(!mediaColumn.getConceptType().equals(ColumnMetadata.ConceptType.ImageV2) && mediaColumn.getParentConceptUuid() == null) {
                    version = 1;
                }
                insertData(tableMetadata, thisTableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime, version);
            });
        });
        deleteDuplicateRows(lastSyncTime);
    }

    private void insertData(TableMetadata mediaTableMetadata, TableMetadata tableMetadata, ColumnMetadata mediaColumn, Date lastSyncTime, Date dataSyncBoundaryTime, int version) {
        syncNewerRows(mediaTableMetadata, tableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime, version);
    }

    private void syncNewerRows(TableMetadata mediaTableMetadata, TableMetadata tableMetadata, ColumnMetadata mediaColumn, Date lastSyncTime, Date dataSyncBoundaryTime, int version) {
        logger.info("Using media template version: " + version);
        String fromTableName = tableMetadata.getName();
        String subjectTypeName = avniMetadataRepository.subjectTypeName(tableMetadata.getSubjectTypeUuid());
        String programName = avniMetadataRepository.programName(tableMetadata.getProgramUuid());
        String encounterTypeName = avniMetadataRepository.encounterTypeName(tableMetadata.getEncounterTypeUuid());
        String conceptName = avniMetadataRepository.conceptName(mediaColumn.getConceptUuid());
        String conceptColumnName = mediaColumn.getName();
        String questionGroupConceptName = mediaColumn.getParentConceptName();
        SyncRegistrationConcept[] syncRegistrationConcepts = avniMetadataRepository.findSyncRegistrationConcepts(tableMetadata.getSubjectTypeUuid());


        tableMetadata.getColumnMetadataList().forEach(columnMetadata -> {
            if (equalsButNotBothNull(columnMetadata.getConceptUuid(), syncRegistrationConcepts[0].getUuid())) {
                syncRegistrationConcepts[0].setColumnName(columnMetadata.getName());
            }

            if (equalsButNotBothNull(columnMetadata.getConceptUuid(), syncRegistrationConcepts[1].getUuid())) {
                syncRegistrationConcepts[1].setColumnName(columnMetadata.getName());
            }
        });
        
        // Select appropriate SQL template based on version
        String sqlTemplate;
        String templateName;
        switch (version) {
            case 2:
            case 3:
                sqlTemplate = mediaV3Sql;
                templateName = "mediaV3.sql.st";
                break;
            case 1:
            default:
                sqlTemplate = mediaSql;
                templateName = "media.sql.st";
                break;
        }
        
        logger.info("Using template file: " + templateName);
        // Dump template content for debugging
        logger.info("Template content:\n" + sqlTemplate);

        ST template = new ST(sqlTemplate)
                .add("schemaName", OrgIdentityContextHolder.getDbSchema())
                .add("tableName", mediaTableMetadata.getName())
                .add("conceptColumnName", conceptColumnName)
                .add("questionGroupConceptName", questionGroupConceptName)
                .add("subjectTypeName", wrapStringValue(subjectTypeName))
                .add("encounterTypeName", wrapStringValue(encounterTypeName))
                .add("programName", wrapStringValue(programName))
                .add("conceptName", wrapStringValue(conceptName))
                .add("fromTableName", fromTableName)
                .add("startTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(lastSyncTime))
                .add("endTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(dataSyncBoundaryTime))
                .add("subjectTableName", subjectTypeTableName(subjectTypeName))
                .add("individualId", tableMetadata.isSubjectTable() ? "id" : "individual_id") // Don't wrap in quotes - template already handles them
                .add("subjectIdColumnName", mediaService.determineSubjectIdColumn(tableMetadata)); // Don't wrap in quotes - template already handles them
        if (syncRegistrationConcepts[0].getUuid() != null) {
            template = template
                    .add("syncRegistrationConcept1Name", wrapStringValue(syncRegistrationConcepts[0].getName()))
                    .add("syncRegistrationConcept1ColumnName", syncRegistrationConcepts[0].getColumnName());
        }
        if (syncRegistrationConcepts[1].getUuid() != null) {
            template = template
                    .add("syncRegistrationConcept2Name", wrapStringValue(syncRegistrationConcepts[1].getName()))
                    .add("syncRegistrationConcept2ColumnName", syncRegistrationConcepts[1].getColumnName());
        }
        if (tableMetadata.getType().equals(TableMetadata.Type.Person) && tableMetadata.hasColumn("middle_name")) {
            template = template
                .add("hasMiddleName", true);
        }

        // Add isRepeatable flag - only true for repeatable question group tables
        template = template
            .add("isRepeatable", tableMetadata.isRepeatableQuestionGroupTable());

        String sql;
        try {
            sql = template.render();
            logger.info("Successfully rendered SQL template");
        } catch (Exception e) {
            logger.error("Error rendering SQL template " + templateName + ": " + e.getMessage(), e);
            throw e;
        }

        runInOrgContext(() -> {
            jdbcTemplate.execute(sql);
            return NullObject.instance();
        }, jdbcTemplate);
    }

    private void deleteDuplicateRows(Date lastSyncTime) {
        String schema = OrgIdentityContextHolder.getDbSchema();
        String sql = new ST(deleteDuplicateMediaSql)
                .add("schemaName", schema)
                .render();
        HashMap<String, Object> params = new HashMap<>();
        params.put("lastSyncTime", lastSyncTime);

        runInOrgContext(() -> {
            new NamedParameterJdbcTemplate(jdbcTemplate).update(sql, params);
            return NullObject.instance();
        }, jdbcTemplate);
    }


    private String subjectTypeTableName(String subjectTypeName) {
        String tableName = new TableNameGenerator().generateName(List.of(subjectTypeName), "IndividualProfile", null);
        return tableName;
    }
    

    private String wrapStringValue(String parameter) {
        return parameter == null ? "null" : "'" + parameter.replace("'", "''") + "'";
    }

    public static boolean equalsButNotBothNull(Object a, Object b) {
        return a != null && b != null && a.equals(b);
    }
}
