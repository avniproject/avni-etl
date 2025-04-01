package org.avniproject.etl.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.ZonedDateTime;

@Repository
public class ETLMetadataRepository {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public ETLMetadataRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public ZonedDateTime getPreviousCutoffDateTime(String schema) {
        try {
            // Set schema for this query
            jdbcTemplate.execute(String.format("SET search_path TO %s", schema));
            
            String sql = "SELECT previous_cutoff_datetime FROM etl_metadata LIMIT 1";
            return jdbcTemplate.queryForObject(sql, ZonedDateTime.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public void updateCutoffDateTime(String schema, ZonedDateTime cutoffDateTime) {
        // Set schema for this query
        jdbcTemplate.execute(String.format("SET search_path TO %s", schema));
        
        // Delete existing record (since we only maintain one record per schema)
        jdbcTemplate.execute("DELETE FROM etl_metadata");
        
        // Insert new record
        String sql = "INSERT INTO etl_metadata (previous_cutoff_datetime) VALUES (?)";
        jdbcTemplate.update(sql, cutoffDateTime);
    }
}
