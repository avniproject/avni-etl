package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
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
public class CalendarDateMarkerTableSyncAction implements EntitySyncAction {

    private final JdbcTemplate jdbcTemplate;
    private static final String sql = readSqlFile("calendarDateMarker.sql.st");

    @Autowired
    public CalendarDateMarkerTableSyncAction(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !tableMetadata.getType().equals(TableMetadata.Type.CalendarDateMarker);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        if (this.doesntSupport(tableMetadata)) {
            return;
        }
        ST template = new ST(sql)
                .add("schemaName", OrgIdentityContextHolder.getDbSchema())
                .add("tableName", tableMetadata.getName())
                .add("startTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(lastSyncTime))
                .add("endTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(dataSyncBoundaryTime));
        String renderedSql = template.render();
        runInOrgContext(() -> {
            jdbcTemplate.execute(renderedSql);
            return NullObject.instance();
        }, jdbcTemplate);
    }
}
