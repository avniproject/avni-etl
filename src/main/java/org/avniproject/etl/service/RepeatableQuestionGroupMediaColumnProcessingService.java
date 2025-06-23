package org.avniproject.etl.service;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.*;
import org.avniproject.etl.repository.rowMappers.tableMappers.EncounterTable;
import org.avniproject.etl.repository.rowMappers.tableMappers.ProgramEncounterTable;
import org.avniproject.etl.repository.rowMappers.tableMappers.ProgramEnrolmentTable;
import org.avniproject.etl.repository.rowMappers.tableMappers.SubjectTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.stringtemplate.v4.ST;

import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
public class RepeatableQuestionGroupMediaColumnProcessingService {
    private static final Logger logger = Logger.getLogger(RepeatableQuestionGroupMediaColumnProcessingService.class);
    private static final String MEDIA_REPEATABLE_GROUP_SQL_TEMPLATE = "mediaRepeatableGroup.sql.st";
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public RepeatableQuestionGroupMediaColumnProcessingService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Process a single media column with its own transaction
     */
    @Transactional
    public void processMediaColumn(TableMetadata mediaTableMetadata, TableMetadata tableData,
                                   ColumnMetadata mediaColumn, Date lastSyncTime, Date dataSyncBoundaryTime) {
        logger.info("Starting transaction for media column: " + mediaColumn.getName());
        String dbSchema = OrgIdentityContextHolder.getDbSchema();
        try {
            jdbcTemplate.execute("SET search_path TO " + dbSchema);
            
            // Process the media column
            syncMediaFromRepeatableQuestionGroup(mediaTableMetadata, tableData, mediaColumn, lastSyncTime, dataSyncBoundaryTime);
            
            logger.info("Transaction completed successfully for media column: " + mediaColumn.getName());
        } catch (Exception e) {
            logger.error("Error in transaction for media column: " + mediaColumn.getName() + ", error: " + e.getMessage(), e);
            throw e; // Re-throw to trigger transaction rollback
        } finally {
            // Reset search path
            try {
                logger.info("Resetting search path to PUBLIC");
                jdbcTemplate.execute(String.format("SET search_path TO %s", "PUBLIC"));
            } catch (Exception e) {
                logger.error("Failed to reset search path in the end", e);
            }
        }
    }
    
    private void syncMediaFromRepeatableQuestionGroup(TableMetadata mediaTableMetadata, TableMetadata tableMetadata,
                                                      ColumnMetadata mediaColumn, Date lastSyncTime, Date dataSyncBoundaryTime) {
        // Get the required parameters for the SQL template
        String dbSchema = OrgIdentityContextHolder.getDbSchema();

        // Format the dates for SQL
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        String lastSyncTimeStr = dateFormat.format(lastSyncTime);
        String dataSyncBoundaryTimeStr = dateFormat.format(dataSyncBoundaryTime);

        String mediaConceptName = mediaColumn.getName();
        logger.info("Syncing media from column: " + mediaColumn.getName() + ", ConceptUUID: " + mediaColumn.getConceptUuid());

        try {
            // Use the mediaRepeatableGroup SQL template
            String sqlTemplate = readSqlFile(MEDIA_REPEATABLE_GROUP_SQL_TEMPLATE);
            ST template = new ST(sqlTemplate);

            // Set all required parameters according to the SQL template
            // Basic schema and table information
            template.add("schemaName", dbSchema);
            template.add("tableName", mediaTableMetadata.getName()); // Target media table
            template.add("fromTableName", tableMetadata.getName());  // Source repeatable group table
            // Pass column name without wrapping in quotes - template will handle quoting
            // For column names with spaces or special characters, this approach ensures proper SQL syntax
            template.add("conceptColumnName", mediaConceptName);

            // Add parent table and subject information based on TableMetadata
            Map<String, Object> parentTableDetails = determineParentTable(tableMetadata);
            String parentTable = (String) parentTableDetails.get("parent_table_name");
            String subjectTypeName = (String) parentTableDetails.get("subject_type_name");
            String encounterTypeName = (String) parentTableDetails.get("encounter_type_name");
            String programName = (String) parentTableDetails.get("program_name");
            String parentIdColumn = determineParentIdColumn(tableMetadata);
            String subjectIdColumn = determineSubjectIdColumn(tableMetadata);

            template.add("parentTableName", parentTable);
            // Don't wrap in quotes - template already handles quoting
            template.add("parentIdColumnName", parentIdColumn);
            // Don't wrap in quotes - template already handles quoting
            template.add("subjectIdColumnName", subjectIdColumn);

            // Pass column name directly, no quoting needed
            template.add("individualId", "individual_id");
            template.add("subjectTableName", "individual");  // Always individual for subject
            template.add("subjectTypeName", "'" + subjectTypeName + "'");
            template.add("encounterTypeName", encounterTypeName != null ? "'" + encounterTypeName + "'" : "null");
            template.add("programName", programName != null ? "'" + programName + "'" : "null");
            template.add("conceptName", "'" + mediaConceptName + "'");


            // Always set mandatory template parameters with default values to avoid "attribute isn't defined" errors
            template.add("formElementUuid", tableMetadata.getName());
            template.add("startTime", lastSyncTimeStr);
            template.add("endTime", dataSyncBoundaryTimeStr);

            // Log template parameters for debugging
            if (logger.isDebugEnabled()) {
                logger.debug("Template parameters:");
                logger.debug("  - schemaName: " + dbSchema);
                logger.debug("  - tableName: " + mediaTableMetadata.getName());
                logger.debug("  - fromTableName: " + tableMetadata.getName());
                logger.debug("  - conceptColumnName: " + mediaConceptName);
                logger.debug("  - subjectTypeName: " + subjectTypeName);
                logger.debug("  - encounterTypeName: " + encounterTypeName);
                logger.debug("  - programName: " + programName);
                logger.debug("  - formElementUuid: " + tableMetadata.getName());
            }

            // Render the SQL with parameters
            String sql;
            try {
                sql = template.render();
                // Log the rendered SQL for debugging
                logger.info("Generated SQL for table: " + tableMetadata.getName() + ", column: " + mediaConceptName);
                if (logger.isDebugEnabled()) {
                    logger.debug("-------- BEGIN SQL --------");
                    logger.debug(sql);
                    logger.debug("--------- END SQL ---------");
                }

                if (sql == null || sql.isEmpty()) {
                    logger.warn("WARNING: Generated SQL is empty or null");
                    return;
                }

                // Execute SQL with proper schema context - removing the hardcoded role setting
                String fullSql = "set search_path to " + dbSchema + "; " + sql;
                jdbcTemplate.execute(fullSql);
                logger.info("Successfully executed SQL for column: " + mediaConceptName);
            } catch (Exception e) {
                logger.error("ERROR rendering SQL template: " + e.getMessage(), e);
                return;
            }
        } catch (Exception e) {
            logger.error("ERROR executing SQL for table: " + tableMetadata.getName() + ", column: " + mediaConceptName, e);
        }
    }

    /**
     * Determines the appropriate parent table for a repeatable question group table based on metadata.
     *
     * @param tableMetadata The metadata for the repeatable question group table
     * @return The name of the parent table
     * @throws IllegalStateException if parent table cannot be determined
     */
    private Map<String, Object> determineParentTable(TableMetadata tableMetadata) {

        // Create a map with all the details needed to generate the parent table name
        Map<String, Object> tableDetails = new HashMap<>();

        // Use parent table type if available - this is the primary method
        TableMetadata.TableType parentTableType = tableMetadata.getParentTableType();
        if (parentTableType != null) {

            // First populate with UUIDs that we definitely have
            if (tableMetadata.getSubjectTypeUuid() != null) {
                tableDetails.put("subject_type_uuid", tableMetadata.getSubjectTypeUuid());
            }
            if (tableMetadata.getProgramUuid() != null) {
                tableDetails.put("program_uuid", tableMetadata.getProgramUuid());
            }
            if (tableMetadata.getEncounterTypeUuid() != null) {
                tableDetails.put("encounter_type_uuid", tableMetadata.getEncounterTypeUuid());
            }

            // Get the current organization context
            String dbUser = OrgIdentityContextHolder.getDbUser();
            String schemaName = OrgIdentityContextHolder.getDbSchema();

            // Query the database for the actual names if we have the subject type UUID
            if (tableMetadata.getSubjectTypeUuid() != null) {
                // For schema-specific queries, we use the schema name and RLS will filter by organization
                String subjectTypeSql = "SELECT ost.name AS subject_type_name FROM public.subject_type st " +
                        "LEFT JOIN public.operational_subject_type ost ON st.id = ost.subject_type_id " +
                        "JOIN public.organisation org ON st.organisation_id = org.id " +
                        "WHERE st.uuid = ? AND (st.is_voided = false OR st.is_voided IS NULL) AND org.schema_name = ?";

                try {
                    // Get subject type name
                    String subjectTypeName = jdbcTemplate.queryForObject(subjectTypeSql, String.class,
                            tableMetadata.getSubjectTypeUuid(), schemaName);

                    if (subjectTypeName != null) {
                        tableDetails.put("subject_type_name", subjectTypeName);
                    } else {
                        // Fallback to using UUID
                        tableDetails.put("subject_type_name", tableMetadata.getSubjectTypeUuid());
                    }
                } catch (Exception e) {
                    // Fallback to using UUID
                    tableDetails.put("subject_type_name", tableMetadata.getSubjectTypeUuid());
                }
            }

            // Query for program name if we have program UUID
            if (tableMetadata.getProgramUuid() != null) {
                // Use schema-qualified table names
                String programSql = "SELECT op.name AS program_name FROM public.program p " +
                        "LEFT JOIN public.operational_program op ON p.id = op.program_id " +
                        "JOIN public.organisation org ON p.organisation_id = org.id " +
                        "WHERE p.uuid = ? AND (p.is_voided = false OR p.is_voided IS NULL) AND org.schema_name = ?";

                try {
                    // Use a separate JdbcTemplate to avoid transaction issues
                    String programName = jdbcTemplate.queryForObject(programSql, String.class, tableMetadata.getProgramUuid(), schemaName);
                    if (programName != null) {
                        tableDetails.put("program_name", programName);
                    } else {
                        // Fallback to using UUID
                        tableDetails.put("program_name", tableMetadata.getProgramUuid());
                    }
                } catch (Exception e) {
                    // Fallback to using UUID
                    tableDetails.put("program_name", tableMetadata.getProgramUuid());
                }
            }

            // Query for encounter type name if we have encounter type UUID
            if (tableMetadata.getEncounterTypeUuid() != null) {
                // Use schema-qualified table names
                String encounterTypeSql = "SELECT oet.name AS encounter_type_name FROM public.encounter_type et " +
                        "LEFT JOIN public.operational_encounter_type oet ON et.id = oet.encounter_type_id " +
                        "JOIN public.organisation org ON et.organisation_id = org.id " +
                        "WHERE et.uuid = ? AND (et.is_voided = false OR et.is_voided IS NULL) AND org.schema_name = ?";

                try {
                    // Get encounter type name
                    String encounterTypeName = jdbcTemplate.queryForObject(encounterTypeSql, String.class,
                            tableMetadata.getEncounterTypeUuid(), schemaName);

                    if (encounterTypeName != null) {
                        tableDetails.put("encounter_type_name", encounterTypeName);
                    } else {
                        tableDetails.put("encounter_type_name", tableMetadata.getEncounterTypeUuid());
                    }
                } catch (Exception e) {
                    logger.error("Error querying for encounter type name: " + e.getMessage(), e);
                    // Fallback to using UUID
                    tableDetails.put("encounter_type_name", tableMetadata.getEncounterTypeUuid());
                }
            }

            // Use the appropriate Table implementation based on parent table type
            String parentTableName;
            try {
                switch (parentTableType) {
                    case ProgramEncounter:
                        parentTableName = new ProgramEncounterTable().name(tableDetails);
                        break;
                    case Encounter:
                        parentTableName = new EncounterTable().name(tableDetails);
                        break;
                    case ProgramEnrolment:
                        parentTableName = new ProgramEnrolmentTable().name(tableDetails);
                        break;
                    case IndividualProfile:
                        // Use SubjectTable for Individual profiles
                        parentTableName = new SubjectTable().name(tableDetails);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported parent table type: " + parentTableType);
                }
                logger.info("Generated parent table name: " + parentTableName);
                tableDetails.put("parent_table_name", parentTableName);
            } catch (Exception e) {
                // If there's an error generating the table name, log it
                logger.error("Error generating parent table name: " + e.getMessage(), e);
                throw e;
            }
        }
        return tableDetails;
    }

    /**
     * Determines the parent ID column name in the repeatable question group table
     * that links to the parent table.
     *
     * @param tableMetadata The metadata for the repeatable question group table
     * @return The name of the parent ID column
     */
    private String determineParentIdColumn(TableMetadata tableMetadata) {

        // Standard ID columns
        if(tableMetadata.getParentTableType() == TableMetadata.TableType.Encounter){
            return "encounter_id";
        }
        if(tableMetadata.getParentTableType() == TableMetadata.TableType.ProgramEncounter){
            return "program_encounter_id";
        }
        if(tableMetadata.getParentTableType() == TableMetadata.TableType.ProgramEnrolment){
            return "program_enrolment_id";
        }
        if(tableMetadata.getParentTableType() == TableMetadata.TableType.IndividualProfile){
            return "subject_id";
        }
        throw new IllegalArgumentException("Unknown parent id column: " + tableMetadata.getParentTableType());
    }

    /**
     * Determines the subject ID column name in the repeatable question group table
     * that links to the subject table.
     *
     * @param tableMetadata The metadata for the repeatable question group table
     * @return The name of the subject ID column
     */
    // Method removed: wrapColumnNameIfNeeded - Template already handles quoting of column names

    private String determineSubjectIdColumn(TableMetadata tableMetadata) {
        // First look for standard subject ID columns
        if (tableMetadata.hasColumn("subject_id")) {
            return "subject_id";
        } else if (tableMetadata.hasColumn("individual_id")) {
            return "individual_id";
        }

        // If the table might itself be a subject table, use the 'id' column
        if (tableMetadata.isSubjectTable()) {
            return "id";
        }

        // Default fallback
        logger.warn("Warning: Could not determine subject ID column for " + tableMetadata.getName() + ". Using 'individual_id' as default.");
        throw new IllegalArgumentException("Unknown subject id column: " + tableMetadata.getSubjectTypeUuid());
    }
}
