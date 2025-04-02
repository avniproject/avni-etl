package org.avniproject.etl.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class PostETLConfig {
    private List<DDLConfig> ddl;
    private List<DMLConfig> dml;

    public List<DDLConfig> getDdl() { return ddl; }
    public void setDdl(List<DDLConfig> ddl) { this.ddl = ddl; }
    public List<DMLConfig> getDml() { return dml; }
    public void setDml(List<DMLConfig> dml) { this.dml = dml; }

    public static class DDLConfig {
        private int order;
        private String table;
        private String sql;
        @JsonProperty("exists_check")
        private String existsCheck;

        public int getOrder() { return order; }
        public void setOrder(int order) { this.order = order; }
        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }
        public String getSql() { return sql; }
        public void setSql(String sql) { this.sql = sql; }
        public String getExistsCheck() { return existsCheck; }
        public void setExistsCheck(String existsCheck) { this.existsCheck = existsCheck; }
    }

    public static class DMLConfig {
        private int order;
        private String table;
        private List<DMLSourceConfig> sqls;

        public int getOrder() { return order; }
        public void setOrder(int order) { this.order = order; }
        public String getTable() { return table; }
        public void setTable(String table) { this.table = table; }
        public List<DMLSourceConfig> getSqls() { return sqls; }
        public void setSqls(List<DMLSourceConfig> sqls) { this.sqls = sqls; }
    }

    public static class DMLSourceConfig {
        private int order;
        @JsonProperty("sourceTableName")
        private String sourceTableName;
        @JsonProperty("insert-sql")
        private String insertSql;
        @JsonProperty("update-sqls")
        private List<String> updateSqls;
        @JsonProperty("sql-params")
        private List<String> sqlParams;

        public int getOrder() { return order; }
        public void setOrder(int order) { this.order = order; }
        public String getSourceTableName() { return sourceTableName; }
        public void setSourceTableName(String sourceTableName) { this.sourceTableName = sourceTableName; }
        public String getInsertSql() { return insertSql; }
        public void setInsertSql(String insertSql) { this.insertSql = insertSql; }
        public List<String> getUpdateSqls() { return updateSqls; }
        public void setUpdateSqls(List<String> updateSqls) { this.updateSqls = updateSqls; }
        public List<String> getSqlParams() { return sqlParams; }
        public void setSqlParams(List<String> sqlParams) { this.sqlParams = sqlParams; }
    }
}
