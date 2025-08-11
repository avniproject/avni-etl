package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.repository.sql.SqlFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

import java.util.List;

import static org.avniproject.etl.repository.sql.SqlFile.readFile;

@Component
public class MultiSelectViewSyncAction {
    private static final Logger logger = LoggerFactory.getLogger(MultiSelectViewSyncAction.class);
    private final JdbcTemplate jdbcTemplate;
    private static final String MULTISELECT_VIEW_SQL = "/sql/etl/view/multiselect_view.sql.st";
    private final String grantViewFile = readFile("/sql/etl/view/grantView.sql.st");

    private static final int POSTGRES_MAX_NAME_LENGTH = 63;
    private static final String VIEW_SUFFIX = "_coded";
    private static final int MAX_TABLE_NAME_LENGTH = POSTGRES_MAX_NAME_LENGTH - VIEW_SUFFIX.length();

    @Autowired
    public MultiSelectViewSyncAction(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        logger.info("Initialized MultiSelectViewSyncAction");
    }

        public void createMultiselectViews(OrganisationIdentity organisationIdentity, SchemaMetadata currentSchemaMetadata) {
        String schemaName = organisationIdentity.getSchemaName();
        try {
            List<TableMetadata> tablesWithMultiselect = currentSchemaMetadata.getOrderedTableMetadata().stream()
                    .filter(table -> table.hasColumnsMatchingConceptType(ColumnMetadata.ConceptType.MultiSelect))
                    .toList();

            logger.info("Found " + tablesWithMultiselect.size() + " tables with multiselect columns");

            for (TableMetadata table : tablesWithMultiselect) {
                try {
                    logger.info("Creating multiselect view for table: " + table.getName());
                    createMultiselectViewForTable(schemaName, table, organisationIdentity.getUsersWithSchemaAccess());
                } catch (Exception e) {
                    logger.error("Error creating multiselect view for table: " + table.getName(), e);
                }
            }
        } catch (Exception e) {
            logger.error("Error in createMultiselectViews", e);
            throw e;
        }
    }

    private void createMultiselectViewForTable(String schemaName, TableMetadata table, List<String> users) {
        List<ColumnMetadata> multiselectColumns = table.findColumnsMatchingConceptType(ColumnMetadata.ConceptType.MultiSelect);
        if (multiselectColumns.isEmpty()) {
            logger.info("No multiselect columns found for table: " + table.getName());
            return;
        }

        logger.info(String.format("Creating multiselect view for table %s with %d multiselect columns", 
                table.getName(), multiselectColumns.size()));

        try {
            String sql = SqlFile.readFile(MULTISELECT_VIEW_SQL);
            ST template = new ST(sql);
            
            // Set schema and table names, ensuring view name stays within PostgreSQL limit
            template.add("schema_name", schemaName);
            String safeViewName = table.getName();
            if (safeViewName.length() > MAX_TABLE_NAME_LENGTH) {
                safeViewName = safeViewName.substring(0, MAX_TABLE_NAME_LENGTH);
            }
            String viewName = safeViewName + VIEW_SUFFIX;
            template.add("table_name", table.getName());
            template.add("view_name", viewName);
            template.add("columns", multiselectColumns);
            
            String generatedSql = template.render();
            logger.debug("Generated coded view for " + table.getName() + ":\n" + generatedSql);
            
            jdbcTemplate.execute(generatedSql);
            logger.info("Successfully created multiselect view for table: " + table.getName());

            users.forEach(user -> grantPermissionToView(schemaName, viewName, user));

        } catch (Exception e) {
            logger.error("Error creating multiselect view for table: " + table.getName(), e);
            throw new RuntimeException("Failed to create multiselect view for table: " + table.getName(), e);
        }
    }

    private void grantPermissionToView(String schemaName, String viewName, String userName) {
        String SCHEMA_PARAM_NAME = "schema_name";
        String VIEW_PARAM_NAME = "view_name";
        String USER_PARAM_NAME = "user_name";
        ST st = new ST(grantViewFile);
        st.add(SCHEMA_PARAM_NAME, schemaName);
        st.add(VIEW_PARAM_NAME, viewName);
        st.add(USER_PARAM_NAME, userName);
        String query = st.render();
        jdbcTemplate.execute(query);
    }
}
