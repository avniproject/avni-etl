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
    private static final String UPSERT_SQL = 
            "INSERT INTO public.post_etl_sync_status (cutoff_datetime, db_user) " +
            "VALUES (?, ?) " +
            "ON CONFLICT ON CONSTRAINT post_etl_sync_status_db_user_key " +
            "DO UPDATE SET cutoff_datetime = EXCLUDED.cutoff_datetime " +
            "WHERE post_etl_sync_status.db_user = ?";
    private static final String SELECT_CUTOFF_SQL = 
            "SELECT cutoff_datetime FROM public.post_etl_sync_status WHERE db_user = ?";
    
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public PostETLSyncStatusRepository(JdbcTemplate jdbcTemplate) {
        Assert.notNull(jdbcTemplate, "JdbcTemplate must not be null");
        this.jdbcTemplate = jdbcTemplate;
    }

    public ZonedDateTime getPreviousCutoffDateTime() {
        try {
            String currentUser = OrgIdentityContextHolder.getDbUser();
            Timestamp timestamp = jdbcTemplate.queryForObject(SELECT_CUTOFF_SQL, Timestamp.class, currentUser);
            if (timestamp == null) {
                log.warn(String.format("No cutoff datetime found for user {}, returning EPOCH", currentUser));
                return ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault());
            }
            return timestamp.toInstant().atZone(ZoneId.systemDefault());
        } catch (DataAccessException e) {
            log.error("Failed to get previous cutoff datetime", e);
            return ZonedDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault());
        }
    }

    public void updateCutoffDateTime(ZonedDateTime dateTime) {
        Assert.notNull(dateTime, "DateTime must not be null");
        try {
            String currentUser = OrgIdentityContextHolder.getDbUser();
            Timestamp timestamp = Timestamp.from(dateTime.toInstant());
            int updated = jdbcTemplate.update(UPSERT_SQL, timestamp, currentUser, currentUser);
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
