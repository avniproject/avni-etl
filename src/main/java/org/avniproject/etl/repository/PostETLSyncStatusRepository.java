package org.avniproject.etl.repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class PostETLSyncStatusRepository {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public PostETLSyncStatusRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void createTableIfNotExists() {
        String createTableSql = "CREATE TABLE IF NOT EXISTS post_etl_sync_status (" +
                "id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1), " +
                "cutoff_datetime TIMESTAMP WITH TIME ZONE NOT NULL)";
        jdbcTemplate.execute(createTableSql);

        // Grant permissions to the current user
        String currentUser = OrgIdentityContextHolder.getDbUser();
        String grantSql = String.format("GRANT ALL PRIVILEGES ON TABLE post_etl_sync_status TO %s", currentUser);
        jdbcTemplate.execute(grantSql);
        
        // Insert initial row if not exists
        String insertSql = "INSERT INTO post_etl_sync_status (id, cutoff_datetime) " +
                "VALUES (1, ?) ON CONFLICT (id) DO NOTHING";
        jdbcTemplate.update(insertSql, Timestamp.from(Instant.EPOCH));
    }

    public ZonedDateTime getPreviousCutoffDateTime() {
        String sql = "SELECT cutoff_datetime FROM post_etl_sync_status WHERE id = 1";
        Timestamp timestamp = jdbcTemplate.queryForObject(sql, Timestamp.class);
        return timestamp.toInstant().atZone(ZoneId.systemDefault());
    }

    public void updateCutoffDateTime(ZonedDateTime dateTime) {
        String sql = "UPDATE post_etl_sync_status SET cutoff_datetime = ? WHERE id = 1";
        jdbcTemplate.update(sql, Timestamp.from(dateTime.toInstant()));
    }
}
