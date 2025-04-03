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
                "id SERIAL PRIMARY KEY, " +
                "cutoff_datetime TIMESTAMP WITH TIME ZONE NOT NULL)";
        jdbcTemplate.execute(createTableSql);

        // Grant permissions to the current user
        String currentUser = OrgIdentityContextHolder.getDbUser();
        String grantSql = String.format("GRANT ALL PRIVILEGES ON TABLE post_etl_sync_status TO %s", currentUser);
        jdbcTemplate.execute(grantSql);
        
        // Also grant permissions on the sequence
        String grantSeqSql = String.format("GRANT USAGE, SELECT ON SEQUENCE post_etl_sync_status_id_seq TO %s", currentUser);
        jdbcTemplate.execute(grantSeqSql);
    }

    public ZonedDateTime getPreviousCutoffDateTime() {
        final ZonedDateTime longPastZonedDateTime = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault());
        String countSql = "SELECT COUNT(*) FROM post_etl_sync_status";
        int count = jdbcTemplate.queryForObject(countSql, Integer.class);
        if (count == 0) {
            return longPastZonedDateTime;
        }
        String sql = "SELECT cutoff_datetime FROM post_etl_sync_status ORDER BY id DESC LIMIT 1";
        Timestamp timestamp = jdbcTemplate.queryForObject(sql, Timestamp.class);
        return timestamp.toInstant().atZone(java.time.ZoneId.systemDefault());
    }

    public void updateCutoffDateTime(ZonedDateTime dateTime) {
        String sql = "INSERT INTO post_etl_sync_status (cutoff_datetime) VALUES (?)";
        jdbcTemplate.update(sql, Timestamp.from(dateTime.toInstant()));
    }
}
