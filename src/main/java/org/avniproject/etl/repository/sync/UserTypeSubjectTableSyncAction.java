package org.avniproject.etl.repository.sync;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Date;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

/**
 * Sync action for User subject type placeholder tables.
 * This handles syncing data from the users table to User subject type tables
 * that don't have IndividualProfile form mappings.
 */
@Repository
public class UserTypeSubjectTableSyncAction implements EntitySyncAction {

    private static final Logger logger = Logger.getLogger(UserTypeSubjectTableSyncAction.class);
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public UserTypeSubjectTableSyncAction(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        // Support User-type placeholder tables (those with subject_type_uuid and user_id column but no form mapping)
        boolean doesntSupport = !tableMetadata.hasColumn("user_id") ||
               tableMetadata.getFormUuid() != null ||
               tableMetadata.getType() != TableMetadata.Type.User ||
               tableMetadata.getSubjectTypeUuid() == null; // Only handle placeholder tables with subject_type_uuid
        
        return doesntSupport;
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        logger.info("Syncing user-type placeholder table: " + tableMetadata.getName());
        syncUserSubjectData(tableMetadata.getName());
    }

    private void syncUserSubjectData(String tableName) {
        String sql = String.format("""
            INSERT INTO %s (id, uuid, user_id, first_name, last_name, address_id, 
                created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, 
                organisation_id, is_voided)
            SELECT 
                u.id,
                u.uuid,
                u.id as user_id,
                u.name as first_name,
                null as last_name,
                null as address_id,
                u.created_by_id,
                u.last_modified_by_id,
                u.created_date_time,
                u.last_modified_date_time,
                u.organisation_id,
                u.is_voided
            FROM public.users u
            WHERE NOT EXISTS (
                SELECT 1 FROM %s ps WHERE ps.user_id = u.id
            );
            """, tableName, tableName);

        try {
            runInOrgContext(() -> {
                // Check if table exists first
                Boolean tableExists = jdbcTemplate.queryForObject(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = ?)", 
                    Boolean.class, tableName);
                
                if (tableExists) {
                    logger.info("Syncing " + jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, Long.class) + " existing records in " + tableName);
                    
                    jdbcTemplate.execute(sql);
                    
                    Long newCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, Long.class);
                    logger.info("Completed sync for " + tableName + ": " + newCount + " total records");
                }
                
                return NullObject.instance();
            }, jdbcTemplate);
        } catch (Exception e) {
            logger.error("Failed to sync user subject data for table " + tableName + ": " + e.getMessage(), e);
        }
    }
}
