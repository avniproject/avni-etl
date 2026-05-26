package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.sql.RepeatableQuestionGroupSyncSqlGenerator;
import org.avniproject.etl.repository.sql.TransactionalSyncSqlGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.String.format;
import static org.avniproject.etl.domain.metadata.TableMetadata.Type.RepeatableQuestionGroup;
import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Repository
public class DuplicateRowDeleteAction implements EntitySyncAction {
    private final JdbcTemplate jdbcTemplate;

    private static final String deleteDuplicateRowsBasedOnId = "delete from \"${schemaName}\".\"${tableName}\"\n" +
            "where id in (\n" +
            "    select t1.id\n" +
            "    from \"${schemaName}\".\"${tableName}\" t1\n" +
            "             inner join \"${schemaName}\".\"${tableName}\" t2 on\n" +
            "                t1.id = t2.id\n" +
            "            and t1.last_modified_date_time < t2.last_modified_date_time)" +
            "   and last_modified_date_time <= '%s';";

    private static final String deleteDuplicateRowsBasedOnParentId = "delete from \"${schemaName}\".\"${tableName}\"\n" +
            "where id in (\n" +
            "    select t1.id\n" +
            "    from \"${schemaName}\".\"${tableName}\" t1\n" +
            "             inner join \"${schemaName}\".\"${tableName}\" t2 on\n" +
            "                t1.${parentIdColumn} = t2.${parentIdColumn}\n" +
            "            and t1.last_modified_date_time < t2.last_modified_date_time)" +
            "   and last_modified_date_time <= '%s';";

    @Autowired
    public DuplicateRowDeleteAction(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !supports(tableMetadata);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        if (this.doesntSupport(tableMetadata)) {
            return;
        }
        deleteDuplicateRows(tableMetadata, lastSyncTime);
    }

    private void deleteDuplicateRows(TableMetadata tableMetadata, Date lastSyncTime) {
        String dateString = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(lastSyncTime);
        boolean isRepeatableQuestionGroup = RepeatableQuestionGroup.equals(tableMetadata.getType());
        String sqlWithFormats = isRepeatableQuestionGroup ? deleteDuplicateRowsBasedOnParentId : deleteDuplicateRowsBasedOnId;
        String baseSql = format(sqlWithFormats, dateString);

        String schema = OrgIdentityContextHolder.getDbSchema();
        String sql = baseSql
                .replace("${schemaName}", schema)
                .replace("${tableName}", tableMetadata.getName());
        if (isRepeatableQuestionGroup) {
            sql = sql.replace("${parentIdColumn}", TableMetadata.qgParentColumnIds.get(tableMetadata.getParentTableType()));
        }
        String finalSql = sql;
        runInOrgContext(() -> {
            jdbcTemplate.execute(finalSql);
            return NullObject.instance();
        }, jdbcTemplate);
    }

    private boolean supports(TableMetadata tableMetadata) {
        return new TransactionalSyncSqlGenerator().supports(tableMetadata) ||
                new RepeatableQuestionGroupSyncSqlGenerator().supports(tableMetadata) ||
                tableMetadata.getType().equals(TableMetadata.Type.User) ||
                tableMetadata.getType().equals(TableMetadata.Type.Address) ||
                isAttendancePassthroughTable(tableMetadata);
    }

    private boolean isAttendancePassthroughTable(TableMetadata tableMetadata) {
        TableMetadata.Type type = tableMetadata.getType();
        return type.equals(TableMetadata.Type.Calendar) ||
                type.equals(TableMetadata.Type.CalendarDateMarker) ||
                type.equals(TableMetadata.Type.AttendanceType) ||
                type.equals(TableMetadata.Type.Session) ||
                type.equals(TableMetadata.Type.AttendanceRecord);
    }
}
