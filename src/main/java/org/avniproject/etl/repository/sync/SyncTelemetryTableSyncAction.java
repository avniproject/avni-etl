package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.text.SimpleDateFormat;
import java.util.Date;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;
import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

@Repository
public class SyncTelemetryTableSyncAction implements EntitySyncAction {
    private final JdbcTemplate jdbcTemplate;
    private static final String syncTelemetrySql = readSqlFile("syncTelemetry.sql.st");

    @Autowired
    public SyncTelemetryTableSyncAction(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !tableMetadata.getType().equals(TableMetadata.Type.SyncTelemetry);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        if (this.doesntSupport(tableMetadata)) {
            return;
        }

        insertData(tableMetadata, lastSyncTime, dataSyncBoundaryTime);
    }

    private void insertData(TableMetadata syncTelemetryTableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime) {
        syncNewerRows(syncTelemetryTableMetadata,lastSyncTime, dataSyncBoundaryTime);

    }

    private void syncNewerRows(TableMetadata syncTelemetryTableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime) {

        ST template = new ST(syncTelemetrySql)
                .add("schemaName", OrgIdentityContextHolder.getDbSchema())
                .add("tableName", syncTelemetryTableMetadata.getName())
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