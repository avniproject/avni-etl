package org.avniproject.etl.repository;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.syncstatus.EntitySyncStatus;
import org.avniproject.etl.domain.syncstatus.SchemaDataSyncStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Repository
public class EntitySyncStatusRepository {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public EntitySyncStatusRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public SchemaDataSyncStatus getSyncStatus() {
        String sql = "select ash.id                as id,\n" +
                "       ash.table_metadata_id as table_metadata_id,\n" +
                "       ash.last_sync_time   as last_sync_time,\n" +
                "       ash.sync_status       as sync_status,\n" +
                "       ash.db_user as db_user,\n" +
                "       ash.schema_name as schema_name\n" +
                "from entity_sync_status ash\n" +
                "where ash.schema_name = :schema";

        Map<String, Object> parameters = new HashMap<>(1);
        parameters.put("schema", OrgIdentityContextHolder.getDbSchema());
        List<EntitySyncStatus> entitySyncStatuses = runInOrgContext(() -> new NamedParameterJdbcTemplate(jdbcTemplate).query(sql, parameters, (rs, rowNum) -> new EntitySyncStatus(
                rs.getInt("id"),
                rs.getInt("table_metadata_id"),
                rs.getTimestamp("last_sync_time"),
                EntitySyncStatus.Status.valueOf(rs.getString("sync_status")),
                rs.getString("db_user"),
                rs.getString("schema_name"))
        ), jdbcTemplate);

        return new SchemaDataSyncStatus(entitySyncStatuses);
    }

    @Transactional
    public EntitySyncStatus save(EntitySyncStatus entitySyncStatus) {
        return entitySyncStatus.getId() == null ?
                insert(entitySyncStatus) :
                update(entitySyncStatus);

    }

    private EntitySyncStatus update(EntitySyncStatus entitySyncStatus) {
        String sql = "update entity_sync_status\n" +
                "set sync_status              = :sync_status,\n" +
                "last_sync_time              = :last_sync_time\n" +
                "where id = :id;";

        new NamedParameterJdbcTemplate(jdbcTemplate).update(sql, addParameters(entitySyncStatus));
        return entitySyncStatus;
    }

    private EntitySyncStatus insert(EntitySyncStatus entitySyncStatus) {
        Number id = new SimpleJdbcInsert(jdbcTemplate).withTableName("entity_sync_status")
                .usingGeneratedKeyColumns("id")
                .executeAndReturnKey(addParameters(entitySyncStatus));

        entitySyncStatus.setId(id.intValue());
        return entitySyncStatus;
    }

    private Map<String, ?> addParameters(EntitySyncStatus entitySyncStatus) {
        Map<String, Object> parameters = new HashMap<>(1);
        parameters.put("id", entitySyncStatus.getId());
        parameters.put("db_user", OrgIdentityContextHolder.getDbUser());
        parameters.put("sync_status", entitySyncStatus.getSyncStatus().toString());
        parameters.put("last_sync_time", entitySyncStatus.getLastSyncTime());
        parameters.put("table_metadata_id", entitySyncStatus.getTableMetadataId());
        parameters.put("schema_name", OrgIdentityContextHolder.getDbSchema());

        return parameters;
    }
}
