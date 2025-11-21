package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;
import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

@Repository
public class UserTableSyncAction implements EntitySyncAction{
    
    private final JdbcTemplate jdbcTemplate;
    private static final String userSql = readSqlFile("user.sql.st");

    @Autowired
    public UserTableSyncAction(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        // Only handle the main users table (subject_type_uuid is null), not placeholder User subject tables
        return !tableMetadata.getType().equals(TableMetadata.Type.User) || 
               !"users".equals(tableMetadata.getName()) ||
               tableMetadata.getSubjectTypeUuid() != null; // placeholder tables have subject_type_uuid
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        if (this.doesntSupport(tableMetadata)) {
            return;
        }

        insertData(tableMetadata, lastSyncTime, dataSyncBoundaryTime);
    }

    private void insertData(TableMetadata userTableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime) {
        syncNewerRows(userTableMetadata,lastSyncTime, dataSyncBoundaryTime);
    }

    private void syncNewerRows(TableMetadata userTableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime) {

        ST template = new ST(userSql)
                .add("schemaName", wrapInQuotes(OrgIdentityContextHolder.getDbSchema()))
                .add("tableName", wrapInQuotes(userTableMetadata.getName()))
                .add("startTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(lastSyncTime))
                .add("endTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(dataSyncBoundaryTime));

        String sql = template.render();

        runInOrgContext(() -> {
            jdbcTemplate.execute(sql);
            return NullObject.instance();
        }, jdbcTemplate);
    }

    private String wrapInQuotes(String parameter) {
        return parameter == null ? "null" : "\"" + parameter + "\"";
    }
}
