package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.TableNameGenerator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

@Repository
public class MediaForRepeatableGroupsSyncAction implements EntitySyncAction {
    private final JdbcTemplate jdbcTemplate;
    private static final String MEDIA_REPEATABLE_GROUP_SQL_TEMPLATE = "mediaRepeatableGroup.sql.st";

    public MediaForRepeatableGroupsSyncAction(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !tableMetadata.getType().equals(TableMetadata.Type.Media);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata schemaMetadata) {
        // Only process if this is a media table sync job
        if (tableMetadata.getType() != TableMetadata.Type.Media) {
            System.out.println("[MEDIA_RQG_SYNC] Skipping sync for non-media table: " + tableMetadata.getName());
            return;
        }

        System.out.println("[MEDIA_RQG_SYNC] Starting sync from repeatable question groups for media table: " + tableMetadata.getName());
        schemaMetadata.getTableMetadata().forEach(table -> {
            boolean isRQG = isRepeatableQuestionGroupTable(table);
            if (isRQG) {
                System.out.println("[MEDIA_RQG_SYNC] Processing repeatable question group table: " + table.getName());
                processRepeatableQuestionGroupTable(tableMetadata, table, lastSyncTime, dataSyncBoundaryTime);
            }
        });
    }

    private boolean isRepeatableQuestionGroupTable(TableMetadata tableMetadata) {
        boolean isRqgType = tableMetadata.getType() == TableMetadata.Type.RepeatableQuestionGroup;
        boolean hasConceptUuid = tableMetadata.getRepeatableQuestionGroupConceptUuid() != null;
        return isRqgType && hasConceptUuid;
    }

    private void processRepeatableQuestionGroupTable(TableMetadata mediaTableMetadata, TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime) {

        // Find media columns by concept types in repeatable question groups
        List<ColumnMetadata> conceptMediaColumns = tableMetadata.findColumnsMatchingConceptType(
                ColumnMetadata.ConceptType.Image,
                ColumnMetadata.ConceptType.ImageV2,
                ColumnMetadata.ConceptType.Video,
                ColumnMetadata.ConceptType.Audio,
                ColumnMetadata.ConceptType.File
        );

        System.out.println("[MEDIA_RQG_SYNC] Found " + conceptMediaColumns.size() + " media columns in table: " + tableMetadata.getName());

        if (conceptMediaColumns.isEmpty()) {
            return; // No media columns found
        }

        // Check if the table has an index column for repeatable group index extraction
        boolean hasIndexColumn = tableMetadata.getColumns().stream()
                .anyMatch(column -> column.getName().equalsIgnoreCase("index"));

        if (!hasIndexColumn) {
            System.out.println("[MEDIA_RQG_SYNC] Warning: Repeatable question group table " + tableMetadata.getName() +
                    " does not have an 'index' column. Will use row_number() as fallback.");
        }

        // Execute the SQL template to extract and store media data
        executeSqlTemplate(mediaTableMetadata, tableMetadata, lastSyncTime, dataSyncBoundaryTime, conceptMediaColumns);
    }

    private void executeSqlTemplate(TableMetadata mediaTableMetadata, TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, List<ColumnMetadata> mediaColumns) {
        mediaColumns.forEach(mediaColumn -> {
            syncMediaFromRepeatableQuestionGroup(mediaTableMetadata, tableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime);
        });
    }

    private void syncMediaFromRepeatableQuestionGroup(TableMetadata mediaTableMetadata, TableMetadata tableMetadata,
                                                       ColumnMetadata mediaColumn, Date lastSyncTime, Date dataSyncBoundaryTime) {
        // Get the required parameters for the SQL template
        String dbSchema = OrgIdentityContextHolder.getDbSchema();

        // Format the dates for SQL
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        String lastSyncTimeStr = dateFormat.format(lastSyncTime);
        String dataSyncBoundaryTimeStr = dateFormat.format(dataSyncBoundaryTime);

        System.out.println("[MEDIA_RQG_SYNC] Syncing media from column: " + mediaColumn.getName() + ", ConceptUUID: " + mediaColumn.getConceptUuid());

        try {
            // Use the mediaRepeatableGroup SQL template
            String sqlTemplate = readSqlFile(MEDIA_REPEATABLE_GROUP_SQL_TEMPLATE);
            ST template = new ST(sqlTemplate);
            
            // Set all required parameters according to the SQL template
            // Basic schema and table information
            template.add("schemaName", dbSchema);
            template.add("tableName", mediaTableMetadata.getName()); // Target media table
            template.add("fromTableName", tableMetadata.getName());  // Source repeatable group table
            template.add("subjectTableName", "individual");  // Always individual for subject
            // Pass column name without wrapping in quotes - template will handle quoting
            // For column names with spaces or special characters, this approach ensures proper SQL syntax
            template.add("conceptColumnName", mediaColumn.getName());
            // Pass column name directly, no quoting needed
            template.add("individualId", "individual_id");
            template.add("subjectTypeName", "'" + tableMetadata.getSubjectTypeUuid() + "'");
            template.add("encounterTypeName", tableMetadata.getEncounterTypeUuid() != null ? "'" + tableMetadata.getEncounterTypeUuid() + "'" : "null");
            template.add("programName", tableMetadata.getProgramUuid() != null ? "'" + tableMetadata.getProgramUuid() + "'" : "null");
            template.add("conceptName", "'" + mediaColumn.getName() + "'");
            
            // Add parent table and subject information based on TableMetadata
            String parentTable = determineParentTable(tableMetadata);
            String parentIdColumn = determineParentIdColumn(tableMetadata);
            String subjectIdColumn = determineSubjectIdColumn(tableMetadata);
            template.add("parentTableName", parentTable);
            // Don't wrap in quotes - template already handles quoting
            template.add("parentIdColumnName", parentIdColumn);
            // Don't wrap in quotes - template already handles quoting
            template.add("subjectIdColumnName", subjectIdColumn);
            
            // Always set mandatory template parameters with default values to avoid "attribute isn't defined" errors
            template.add("hasMiddleName", true);
            template.add("formElementUuid", mediaColumn.getConceptUuid());
            template.add("syncRegistrationConcept1Name", null);
            template.add("syncRegistrationConcept1ColumnName", null);
            template.add("syncRegistrationConcept2Name", null);
            template.add("syncRegistrationConcept2ColumnName", null);
            template.add("startTime", lastSyncTimeStr);
            template.add("endTime", dataSyncBoundaryTimeStr);

            // Log template parameters for debugging
            System.out.println("[MEDIA_RQG_SYNC] Template parameters:");
            System.out.println("  - schemaName: " + dbSchema);
            System.out.println("  - tableName: " + mediaTableMetadata.getName());
            System.out.println("  - fromTableName: " + tableMetadata.getName());
            System.out.println("  - conceptColumnName: " + mediaColumn.getName());
            System.out.println("  - subjectTypeName: " + tableMetadata.getSubjectTypeUuid());
            System.out.println("  - encounterTypeName: " + tableMetadata.getEncounterTypeUuid());
            System.out.println("  - programName: " + tableMetadata.getProgramUuid());
            System.out.println("  - formElementUuid: " + mediaColumn.getConceptUuid());

            // Render the SQL with parameters
            String sql;
            try {
                sql = template.render();
                // Log the rendered SQL for debugging
                System.out.println("[MEDIA_RQG_SYNC] Generated SQL for table: " + tableMetadata.getName() + ", column: " + mediaColumn.getName());
                System.out.println("-------- BEGIN SQL --------");
                System.out.println(sql);
                System.out.println("--------- END SQL ---------");

                if (sql == null || sql.isEmpty()) {
                    System.out.println("[MEDIA_RQG_SYNC] WARNING: Generated SQL is empty or null");
                    return;
                }

                // Execute SQL with proper schema context
                String fullSql = "set search_path to " + dbSchema + "; set role dineshbootcamp; " + sql;
                jdbcTemplate.execute(fullSql);
                System.out.println("[MEDIA_RQG_SYNC] Successfully executed SQL for column: " + mediaColumn.getName());
            } catch (Exception e) {
                System.out.println("[MEDIA_RQG_SYNC] ERROR rendering SQL template: " + e.getMessage());
                e.printStackTrace();
                return;
            }



        } catch (Exception e) {
            System.out.println("[MEDIA_RQG_SYNC] ERROR executing SQL for table: " + tableMetadata.getName() + ", column: " + mediaColumn.getName());
            System.out.println("[MEDIA_RQG_SYNC] Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Determines the appropriate parent table for a repeatable question group table based on metadata.
     * Uses TableNameGenerator to generate the correct subject-specific table name.
     * 
     * @param tableMetadata The metadata for the repeatable question group table
     * @return The name of the parent table
     */
    private String determineParentTable(TableMetadata tableMetadata) {
        // Extract the subject type UUID from the RQG table name
        String subjectTypeUuid = tableMetadata.getSubjectTypeUuid();
        TableNameGenerator tableNameGenerator = new TableNameGenerator();
        
        // Check various parent table possibilities based on ID columns in the table
        if (tableMetadata.hasColumn("program_encounter_id")) {
            String programUuid = tableMetadata.getProgramUuid();
            String encounterTypeUuid = tableMetadata.getEncounterTypeUuid();
            if (programUuid != null && encounterTypeUuid != null) {
                return tableNameGenerator.generateName(List.of(subjectTypeUuid, programUuid, encounterTypeUuid), "ProgramEncounter", null);
            }
            System.out.println("[MEDIA_RQG_SYNC] Warning: Missing program or encounter type UUID for " + tableMetadata.getName());
            return "individual_program_encounter"; // Generic fallback
        } else if (tableMetadata.hasColumn("encounter_id")) {
            String encounterTypeUuid = tableMetadata.getEncounterTypeUuid();
            if (subjectTypeUuid != null && encounterTypeUuid != null) {
                return tableNameGenerator.generateName(List.of(subjectTypeUuid, encounterTypeUuid), "Encounter", null);
            }
            System.out.println("[MEDIA_RQG_SYNC] Warning: Missing encounter type UUID for " + tableMetadata.getName());
            return "individual_encounter"; // Generic fallback
        } else if (tableMetadata.hasColumn("program_enrolment_id")) {
            String programUuid = tableMetadata.getProgramUuid();
            if (subjectTypeUuid != null && programUuid != null) {
                return tableNameGenerator.generateName(List.of(subjectTypeUuid, programUuid), "ProgramEnrolment", null);
            }
            System.out.println("[MEDIA_RQG_SYNC] Warning: Missing program UUID for " + tableMetadata.getName());
            return "individual_program"; // Generic fallback
        } else if (tableMetadata.hasColumn("subject_id") || tableMetadata.hasColumn("individual_id")) {
            if (subjectTypeUuid != null) {
                return tableNameGenerator.generateName(List.of(subjectTypeUuid), "IndividualProfile", null);
            }
            System.out.println("[MEDIA_RQG_SYNC] Warning: Missing subject type UUID for " + tableMetadata.getName());
            return "individual"; // Generic fallback
        }
        
        // Default fallback - use individual as parent
        System.out.println("[MEDIA_RQG_SYNC] Warning: Could not determine parent table for " + tableMetadata.getName() + ". Using 'individual' as default.");
        return "individual";
    }
    
    /**
     * Determines the parent ID column name in the repeatable question group table
     * that links to the parent table.
     * 
     * @param tableMetadata The metadata for the repeatable question group table
     * @return The name of the parent ID column
     */
    private String determineParentIdColumn(TableMetadata tableMetadata) {
        // First try to find columns with standard names
        String parentTableName = determineParentTable(tableMetadata);
        
        // Look for columns that link to the parent table
        for (ColumnMetadata column : tableMetadata.getColumnMetadataList()) {
            String columnName = column.getName();
            // Standard ID columns
            if (columnName.equals("program_encounter_id") || 
                columnName.equals("encounter_id") ||
                columnName.equals("program_enrolment_id") ||
                columnName.equals("subject_id") ||
                columnName.equals("individual_id")) {
                return columnName;
            }
            
            // Check for foreign key columns that match the parent table name pattern
            // For example: individual_child_test_enc_id for parent table individual_child_test_enc
            if (columnName.endsWith("_id") && parentTableName != null) {
                String possibleTableName = columnName.substring(0, columnName.length() - 3); // Remove _id suffix
                if (possibleTableName.equals(parentTableName) || 
                    parentTableName.contains(possibleTableName) || 
                    possibleTableName.contains(parentTableName)) {
                    return columnName;
                }
            }
        }
        
        // If we couldn't determine by name, check for columns with ID naming conventions
        for (ColumnMetadata column : tableMetadata.getColumnMetadataList()) {
            // Check if it's a likely foreign key column (ends with _id)
            if (column.getName().endsWith("_id")) {
                return column.getName();
            }
        }
        
        // Default fallbacks based on standard naming patterns
        if (tableMetadata.hasColumn("program_encounter_id")) {
            return "program_encounter_id";
        } else if (tableMetadata.hasColumn("encounter_id")) {
            return "encounter_id";
        } else if (tableMetadata.hasColumn("program_enrolment_id")) {
            return "program_enrolment_id";
        } else if (tableMetadata.hasColumn("subject_id")) {
            return "subject_id";
        } else if (tableMetadata.hasColumn("individual_id")) {
            return "individual_id";
        }
        
        // Last resort fallback
        System.out.println("[MEDIA_RQG_SYNC] Warning: Could not determine parent ID column for " + tableMetadata.getName() + ". Using 'individual_id' as default.");
        return "individual_id";
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
        
        // Look for columns that might be the subject ID based on name patterns
        for (ColumnMetadata column : tableMetadata.getColumnMetadataList()) {
            String columnName = column.getName();
            // Check for column names that suggest they are subject IDs
            if ((columnName.contains("subject") && columnName.contains("id")) ||
                (columnName.contains("individual") && columnName.contains("id"))) {
                return columnName;
            }
        }
        
        // If the table might itself be a subject table, use the 'id' column
        if (tableMetadata.isSubjectTable()) {
            return "id";
        }
        
        // Default fallback
        System.out.println("[MEDIA_RQG_SYNC] Warning: Could not determine subject ID column for " + tableMetadata.getName() + ". Using 'individual_id' as default.");
        return "individual_id";
    }
}
