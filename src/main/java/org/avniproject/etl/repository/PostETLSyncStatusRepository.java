package org.avniproject.etl.repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.ZonedDateTime;

@Repository
public class PostETLSyncStatusRepository implements PostETLSyncStatusKeys {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public PostETLSyncStatusRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = new ObjectMapper();
    }

    public void createTableIfNotExists() {
        String sql = "CREATE TABLE IF NOT EXISTS post_etl_sync_status (" +
                    "key TEXT PRIMARY KEY, " +
                    "value JSONB)";
        jdbcTemplate.execute(sql);
    }

    public ZonedDateTime getPreviousCutoffDateTime() {
        try {
            String sql = "SELECT value::text FROM post_etl_sync_status WHERE key = ?";
            String jsonValue = jdbcTemplate.queryForObject(sql, String.class, CUTOFF_TIME_KEY);
            if (jsonValue == null) return null;
            
            // Parse the timestamp string from JSON
            JsonNode node = objectMapper.readTree(jsonValue);
            if (node == null || !node.isTextual()) return null;
            
            return ZonedDateTime.parse(node.asText());
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Error parsing timestamp from JSON", e);
        }
    }

    public void updateCutoffDateTime(ZonedDateTime cutoffDateTime) {
        try {
            // Create a JSON string value for the timestamp
            String jsonValue = objectMapper.writeValueAsString(TextNode.valueOf(cutoffDateTime.toString()));
            
            String sql = "INSERT INTO post_etl_sync_status (key, value) " +
                        "VALUES (?, ?::jsonb) " +
                        "ON CONFLICT (key) DO UPDATE " +
                        "SET value = EXCLUDED.value";
            jdbcTemplate.update(sql, CUTOFF_TIME_KEY, jsonValue);
        } catch (Exception e) {
            throw new RuntimeException("Error storing timestamp as JSON", e);
        }
    }
    
    /**
     * Generic method to store any value as JSON
     */
    public <T> void setValue(String key, T value) {
        try {
            String jsonValue = objectMapper.writeValueAsString(value);
            
            String sql = "INSERT INTO post_etl_sync_status (key, value) " +
                        "VALUES (?, ?::jsonb) " +
                        "ON CONFLICT (key) DO UPDATE " +
                        "SET value = EXCLUDED.value";
            jdbcTemplate.update(sql, key, jsonValue);
        } catch (Exception e) {
            throw new RuntimeException("Error storing value as JSON", e);
        }
    }
    
    /**
     * Generic method to retrieve a JSON value and convert it to the specified type
     */
    public <T> T getValue(String key, Class<T> type) {
        try {
            String sql = "SELECT value::text FROM post_etl_sync_status WHERE key = ?";
            String jsonValue = jdbcTemplate.queryForObject(sql, String.class, key);
            if (jsonValue == null) return null;
            
            return objectMapper.readValue(jsonValue, type);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            throw new RuntimeException("Error parsing JSON value", e);
        }
    }
}
