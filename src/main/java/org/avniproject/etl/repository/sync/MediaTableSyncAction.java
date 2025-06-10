package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.result.SyncRegistrationConcept;
import org.avniproject.etl.repository.AvniMetadataRepository;
import org.avniproject.etl.repository.rowMappers.TableNameGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;
import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

@Repository
public class MediaTableSyncAction implements EntitySyncAction {
    private static final Logger logger = Logger.getLogger(MediaTableSyncAction.class.getName());
    private final JdbcTemplate jdbcTemplate;
    private final AvniMetadataRepository avniMetadataRepository;
    private static final String mediaSql = readSqlFile("media.sql.st");
    private static final String mediaV2Sql = readSqlFile("mediaV2.sql.st");
    private static final String mediaV3Sql = readSqlFile("mediaV3.sql.st");
    private static final String deleteDuplicateMediaSql = readSqlFile("deleteDuplicateMedia.sql.st");

    @Autowired
    public MediaTableSyncAction(JdbcTemplate jdbcTemplate, AvniMetadataRepository metadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.avniMetadataRepository = metadataRepository;
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
            if (thisTableMetadata.getName().endsWith("_repeatable")) {
                return;
            }
            
            // Legacy Image columns use the original SQL format
            List<ColumnMetadata> legacyMediaColumns = thisTableMetadata.findColumnsMatchingConceptType(ColumnMetadata.ConceptType.Image);
            legacyMediaColumns.forEach(mediaColumn -> {
                insertData(tableMetadata, thisTableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime, 1);
            });
            
            // All modern media types use V3 SQL which includes form element context for QuestionGroups
            // ImageV2 columns
            List<ColumnMetadata> mediaV2Columns = thisTableMetadata.findColumnsMatchingConceptType(ColumnMetadata.ConceptType.ImageV2);
            mediaV2Columns.forEach(mediaColumn -> {
                insertData(tableMetadata, thisTableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime, 3);
            });
            
            // Audio, Video, and File columns
            List<ColumnMetadata> otherMediaColumns = thisTableMetadata.findColumnsMatchingConceptType(
                ColumnMetadata.ConceptType.Video, 
                ColumnMetadata.ConceptType.Audio, 
                ColumnMetadata.ConceptType.File);
            otherMediaColumns.forEach(mediaColumn -> {
                insertData(tableMetadata, thisTableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime, 3);
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
            case 1:
                sqlTemplate = mediaSql;
                templateName = "media.sql.st";
                break;
            case 2:
                sqlTemplate = mediaV2Sql;
                templateName = "mediaV2.sql.st";
                break;
            case 3:
                sqlTemplate = mediaV3Sql;
                templateName = "mediaV3.sql.st";
                break;
            default:
                sqlTemplate = mediaSql;
                templateName = "media.sql.st";
        }
        
        logger.info("Using template file: " + templateName);
        // Dump template content for debugging
        logger.info("Template content:\n" + sqlTemplate);

        ST template = new ST(sqlTemplate)
                .add("schemaName", wrapInQuotes(OrgIdentityContextHolder.getDbSchema()))
                .add("tableName", wrapInQuotes(mediaTableMetadata.getName()))
                .add("conceptColumnName", conceptColumnName)
                .add("subjectTypeName", wrapStringValue(subjectTypeName))
                .add("encounterTypeName", wrapStringValue(encounterTypeName))
                .add("programName", wrapStringValue(programName))
                .add("conceptName", wrapStringValue(conceptName))
                .add("fromTableName", wrapInQuotes(fromTableName))
                .add("startTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(lastSyncTime))
                .add("endTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(dataSyncBoundaryTime))
                .add("subjectTableName", subjectTypeTableName(subjectTypeName))
                .add("individualId", tableMetadata.isSubjectTable() ? "id" : "individual_id") // Don't wrap in quotes - template already handles them
                .add("subjectIdColumnName", determineSubjectIdColumn(tableMetadata)); // Don't wrap in quotes - template already handles them
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

        String sql;
        try {
            sql = template.render();
            logger.info("Successfully rendered SQL template");
        } catch (Exception e) {
            logger.severe("Error rendering SQL template " + templateName + ": " + e.getMessage());
            e.printStackTrace();
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
        return new TableNameGenerator().generateName(List.of(subjectTypeName), "IndividualProfile", null);
    }
    
    /**
     * Determines the appropriate subject ID column to use for joining with the subject table.
     * Handles special cases like when the entity table is the subject table itself.
     * 
     * @param tableMetadata The metadata for the table we're working with
     * @return The name of the column to use for the subject ID join
     */
    private String determineSubjectIdColumn(TableMetadata tableMetadata) {
        // If this is the individual/subject table itself, use 'id' for self-join
        if (tableMetadata.isSubjectTable() || "individual".equals(tableMetadata.getName())) {
            return "id";
        }
        
        // Check if table has subject_id column
        if (tableMetadata.hasColumn("subject_id")) {
            return "subject_id";
        }
        
        // Default to individual_id for backward compatibility
        return "individual_id";
    }

    /**
     * Properly wraps a value in double quotes if needed
     * - Doesn't quote template variables (<variable>)
     * - Always quotes column names with spaces or special characters
     * - Always quotes column names that aren't alphanumeric+underscore
     * 
     * @param value The value to potentially wrap in quotes
     * @return The quoted or unquoted value as appropriate
     */
    private String wrapInQuotes(String value) {
        // Don't add quotes for template variables that will be substituted
        if (value != null && value.startsWith("<") && value.endsWith(">")) {
            return value;
        }
        
        // Always quote if value is null, contains spaces, special chars, or isn't a valid identifier
        if (value == null || value.contains(" ") || value.contains("-") || 
            !value.matches("^[a-zA-Z0-9_]*$")) {
            return value == null ? null : ("\"" + value + "\"");
        }
        
        // If it's a PostgreSQL reserved word, quote it
        String lowerValue = value.toLowerCase();
        if (PostgreSQLReservedWords.isReserved(lowerValue)) {
            return "\"" + value + "\"";
        }
        
        // For regular column names, don't add quotes
        return value;
    }
    
    /**
     * Helper class to check if a word is a PostgreSQL reserved word
     */
    private static class PostgreSQLReservedWords {
        private static final String[] RESERVED_WORDS = {
            "all", "analyse", "analyze", "and", "any", "array", "as", "asc", 
            "asymmetric", "authorization", "binary", "both", "case", "cast", 
            "check", "collate", "column", "constraint", "create", "cross", 
            "current_catalog", "current_date", "current_role", "current_schema", 
            "current_time", "current_timestamp", "current_user", "default", 
            "deferrable", "desc", "distinct", "do", "else", "end", "except", 
            "false", "fetch", "for", "foreign", "freeze", "from", "full", 
            "grant", "group", "having", "ilike", "in", "initially", "inner", 
            "intersect", "into", "is", "isnull", "join", "lateral", "leading", 
            "left", "like", "limit", "localtime", "localtimestamp", "natural", 
            "not", "notnull", "null", "offset", "on", "only", "or", "order", 
            "outer", "overlaps", "placing", "primary", "references", "returning", 
            "right", "select", "session_user", "similar", "some", "symmetric", 
            "table", "then", "to", "trailing", "true", "union", "unique", "user", 
            "using", "variadic", "verbose", "when", "where", "window", "with"
        };
        
        public static boolean isReserved(String word) {
            if (word == null) return false;
            for (String reserved : RESERVED_WORDS) {
                if (reserved.equals(word)) {
                    return true;
                }
            }
            return false;
        }
    }

    private String wrapStringValue(String parameter) {
        return parameter == null ? "null" : "'" + parameter.replace("'", "''") + "'";
    }

    public static boolean equalsButNotBothNull(Object a, Object b) {
        return a != null && b != null && a.equals(b);
    }
}
