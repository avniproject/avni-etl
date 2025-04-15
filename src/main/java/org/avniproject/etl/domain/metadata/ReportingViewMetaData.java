package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.OrganisationIdentity;

public interface ReportingViewMetaData {
    String SCHEMA_PARAM_NAME = "schema_name";
    String VIEW_PARAM_NAME = "view_name";
    String ADDRESS_COLUMNS_PARAM_NAME = "address_columns";
    String WHERE_CLAUSE = "where_clause";
    String EXTRA_COLUMNS = "extra_columns";
    String USER_PARAM_NAME = "user_name";
    String DB_USER = "db_user";


    void createOrReplaceView(OrganisationIdentity organisationIdentity);

    class ViewConfig {
        private final String viewName;
        private final String whereClause;
        private final String extraColumns;
        private final String sqlTemplateFile;

        public ViewConfig(String viewName, String whereClause, String extraColumns, String sqlTemplateFile) {
            this.viewName = viewName;
            this.whereClause = whereClause;
            this.extraColumns = extraColumns != null ? extraColumns : "";
            this.sqlTemplateFile = sqlTemplateFile;
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
    }
}
