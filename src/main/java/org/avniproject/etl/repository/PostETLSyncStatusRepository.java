package org.avniproject.etl.repository;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

@Repository
@Transactional
public class PostETLSyncStatusRepository {
    private static final Logger log = Logger.getLogger(PostETLSyncStatusRepository.class);
    
    private static final String CREATE_TABLE_SQL = 
            "CREATE TABLE IF NOT EXISTS post_etl_sync_status (" +
            "id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1), " +
            "cutoff_datetime TIMESTAMP WITH TIME ZONE NOT NULL)";
    
    private static final String INSERT_INITIAL_ROW_SQL = 
            "INSERT INTO post_etl_sync_status (id, cutoff_datetime) " +
            "VALUES (1, ?) ON CONFLICT (id) DO NOTHING";
    
    private static final String SELECT_CUTOFF_SQL = 
            "SELECT cutoff_datetime FROM post_etl_sync_status WHERE id = 1";
    
    private static final String UPDATE_CUTOFF_SQL = 
            "UPDATE post_etl_sync_status SET cutoff_datetime = ? WHERE id = 1";
    
    private static final String GRANT_PERMISSIONS_SQL = 
            "GRANT ALL PRIVILEGES ON TABLE post_etl_sync_status TO %s";

    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public PostETLSyncStatusRepository(JdbcTemplate jdbcTemplate) {
        Assert.notNull(jdbcTemplate, "JdbcTemplate must not be null");
        this.jdbcTemplate = jdbcTemplate;
    }

    public void createTableIfNotExists() {
        try {
            jdbcTemplate.execute(CREATE_TABLE_SQL);

            // Grant permissions to the current user
            String currentUser = OrgIdentityContextHolder.getDbUser();
            String grantSql = String.format(GRANT_PERMISSIONS_SQL, currentUser);
            jdbcTemplate.execute(grantSql);
            
            // Insert initial row with EPOCH timestamp
            jdbcTemplate.update(INSERT_INITIAL_ROW_SQL, Timestamp.from(Instant.EPOCH));
        } catch (DataAccessException e) {
            log.error("Failed to create post_etl_sync_status table", e);
            throw new PostETLSyncException("Failed to create post_etl_sync_status table", e);
        }
    }

    public ZonedDateTime getPreviousCutoffDateTime() {
        try {
            Timestamp timestamp = jdbcTemplate.queryForObject(SELECT_CUTOFF_SQL, Timestamp.class);
            if (timestamp == null) {
                log.warn("No cutoff datetime found, returning EPOCH");
                return ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault());
            }
            return timestamp.toInstant().atZone(ZoneId.systemDefault());
        } catch (DataAccessException e) {
            log.error("Failed to get previous cutoff datetime", e);
            throw new PostETLSyncException("Failed to get previous cutoff datetime", e);
        }
    }

    public void updateCutoffDateTime(ZonedDateTime dateTime) {
        Assert.notNull(dateTime, "DateTime must not be null");
        try {
            int updated = jdbcTemplate.update(UPDATE_CUTOFF_SQL, Timestamp.from(dateTime.toInstant()));
            if (updated != 1) {
                throw new PostETLSyncException("Failed to update cutoff datetime: no row was updated");
            }
        } catch (DataAccessException e) {
            log.error("Failed to update cutoff datetime", e);
            throw new PostETLSyncException("Failed to update cutoff datetime", e);
        }
    }
}

class PostETLSyncException extends RuntimeException {
    public PostETLSyncException(String message) {
        super(message);
    }
    
    public PostETLSyncException(String message, Throwable cause) {
        super(message, cause);
    }
}
