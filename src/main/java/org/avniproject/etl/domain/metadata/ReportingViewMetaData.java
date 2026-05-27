package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.OrganisationIdentity;

import java.util.List;

public interface ReportingViewMetaData {
    String SCHEMA_PARAM_NAME = "schema_name";
    String VIEW_PARAM_NAME = "view_name";
    String ADDRESS_COLUMNS_PARAM_NAME = "address_columns";
    String WHERE_CLAUSE = "where_clause";
    String EXTRA_COLUMNS = "extra_columns";
    String USER_PARAM_NAME = "user_name";
    String TABLE_METADATA = "table_metadata";
    String DB_USER = "db_user";
    String STUDENT_WINDOW_MONTHS = "student_window_months";


    void createOrReplaceView(OrganisationIdentity organisationIdentity, SchemaMetadata schemaMetadata);

    class ViewConfig {
        private final String viewName;
        private final String whereClause;
        private final String extraColumns;
        private final String sqlTemplateFile;
        private final List<TableMetadata.Type> metadataTypes;
        private final boolean materialized;
        private final boolean gated;

        public ViewConfig(String viewName, String whereClause, String extraColumns, String sqlTemplateFile,
                          List<TableMetadata.Type> metadataTypes) {
            this(viewName, whereClause, extraColumns, sqlTemplateFile, metadataTypes, false, false);
        }

        public ViewConfig(String viewName, String whereClause, String extraColumns, String sqlTemplateFile,
                          List<TableMetadata.Type> metadataTypes, boolean materialized, boolean gated) {
            this.viewName = viewName;
            this.whereClause = whereClause;
            this.extraColumns = extraColumns != null ? extraColumns : "";
            this.sqlTemplateFile = sqlTemplateFile;
            this.metadataTypes = metadataTypes != null ? metadataTypes : List.of();
            this.materialized = materialized;
            this.gated = gated;
        }

        public String getViewName() {
            return viewName;
        }
        public String getWhereClause() {
            return whereClause;
        }
        public String getExtraColumns() {
            return extraColumns;
        }
        public String getSqlTemplateFile() {
            return sqlTemplateFile;
        }
        public List<TableMetadata.Type> getMetadataTypes() {
            return metadataTypes;
        }
        public boolean isMaterialized() {
            return materialized;
        }
        public boolean isGated() {
            return gated;
        }
    }
}
