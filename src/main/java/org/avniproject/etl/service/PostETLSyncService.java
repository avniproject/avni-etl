package org.avniproject.etl.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.PostETLConfig;
import org.avniproject.etl.repository.PostETLSyncStatusRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;

@Service
public class PostETLSyncService {
    private static final Logger log = Logger.getLogger(PostETLSyncService.class);
    private static final String CONFIG_FILE_NAME = "post-etl-sync-processing-config.json";
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final PostETLSyncStatusRepository postETLSyncStatusRepository;

    @Autowired
    public PostETLSyncService(JdbcTemplate jdbcTemplate, PostETLSyncStatusRepository postETLSyncStatusRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = new ObjectMapper();
        this.postETLSyncStatusRepository = postETLSyncStatusRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void executePostETLScripts(Organisation organisation) {
        try {
            String schema = OrgIdentityContextHolder.getDbSchema();
            log.info("Starting post-ETL SQL script execution for organisation schema: " + schema);
            
            // Set schema
            jdbcTemplate.execute(String.format("SET search_path TO %s", schema));

            // Create post_etl_sync_status table if it doesn't exist
            postETLSyncStatusRepository.createTableIfNotExists();
            
            // Load and execute config
            PostETLConfig config = loadConfig(organisation);
            if (config == null) {
                log.info("No post-ETL config found for organisation: " + organisation.getOrganisationIdentity().getSchemaName());
                return;
            }

            ZonedDateTime previousCutoffDateTime = postETLSyncStatusRepository.getPreviousCutoffDateTime();
            ZonedDateTime newCutoffDateTime = ZonedDateTime.now();

            // Execute DDL scripts first
            processDDLSqls(organisation, config);

            // Execute DML scripts in order
            processDMLSqls(organisation, config, previousCutoffDateTime, newCutoffDateTime);

            // Update the cutoff datetime for next run
            postETLSyncStatusRepository.updateCutoffDateTime(newCutoffDateTime);
            
            log.info("Completed post-ETL SQL script execution");
        } catch (Throwable t) {
            log.error("Error executing post-ETL scripts", t);
            throw new RuntimeException("Failed to execute post-ETL scripts", t);
        } finally {
            // Reset search path
            try {
                jdbcTemplate.execute(String.format("SET search_path TO %s", "PUBLIC"));
            } catch (Exception e) {
                log.error("Failed to reset search path in the end", e);
            }
        }
    }

    private void processDDLSqls(Organisation organisation, PostETLConfig config) {
        if (config.getDdl() == null)
            return;

        // Sort DDL configs based on order before processing
        config.getDdl().stream().sorted(Comparator.comparingInt(PostETLConfig.DDLConfig::getOrder)).forEach(ddl -> {
            if (!tableExists(ddl)) {
                executeSqlFile(organisation, ddl.getSql());
            } else {
                log.info("Table " + ddl.getTable() + " already exists, skipping DDL");
            }
        });
    }

    private void processDMLSqls(Organisation organisation, PostETLConfig config, ZonedDateTime previousCutoffDateTime, ZonedDateTime newCutoffDateTime) {
        if (config.getDml() == null) return;

        // Sort DML configs based on order before processing
        config.getDml().stream()
                .sorted(Comparator.comparingInt(PostETLConfig.DMLConfig::getOrder))
                .forEach(dml -> {
                    if (dml.getSqls() != null) {
                        dml.getSqls().stream()
                                .sorted(Comparator.comparingInt(PostETLConfig.DMLSourceConfig::getOrder))
                                .forEach(sourceConfig -> {
                                    // Execute insert SQL if present
                                    if (StringUtils.hasText(sourceConfig.getInsertSql())) {
                                        executeSqlFileWithParams(organisation, 
                                                sourceConfig.getInsertSql(), 
                                                previousCutoffDateTime, 
                                                newCutoffDateTime, 
                                                sourceConfig.getSqlParams());
                                    }
                                    // Execute update SQLs in order
                                    if (sourceConfig.getUpdateSqls() != null) {
                                        sourceConfig.getUpdateSqls()
                                                .forEach(sql -> executeSqlFileWithParams(organisation, 
                                                        sql, 
                                                        previousCutoffDateTime, 
                                                        newCutoffDateTime, 
                                                        sourceConfig.getSqlParams()));
                                    }
                                });
                    }
                });
    }

    private boolean tableExists(PostETLConfig.DDLConfig ddl) {
        if (StringUtils.hasText(ddl.getExistsCheck())) {
            return jdbcTemplate.queryForObject(ddl.getExistsCheck(), Boolean.class);
        }
        // Default check using information_schema
        String sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)";
        return Boolean.TRUE.equals(jdbcTemplate.queryForObject(sql, Boolean.class, ddl.getTable().toLowerCase()));
    }

    private PostETLConfig loadConfig(Organisation organisation) {
        try {
            String configPath = String.format("post-etl/%s/%s", organisation.getOrganisationIdentity().getSchemaName().toLowerCase(), CONFIG_FILE_NAME);
            ClassPathResource resource = new ClassPathResource(configPath);
            if (!resource.exists()) {
                return null;
            }
            try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
                String configJson = FileCopyUtils.copyToString(reader);
                return objectMapper.readValue(configJson, PostETLConfig.class);
            }
        } catch (IOException e) {
            log.error("Error loading post-ETL config", e);
            throw new RuntimeException("Failed to load post-ETL config", e);
        }
    }

    private void executeSqlFile(Organisation organisation, String sqlFileName) {
        executeSqlFileWithParams(organisation, sqlFileName, null, null, null);
    }

    private void executeSqlFileWithParams(Organisation organisation, String sqlFileName, 
            ZonedDateTime previousCutoffDateTime, ZonedDateTime newCutoffDateTime, List<String> additionalParams) {
        try {
            String sqlPath = String.format("post-etl/%s/%s", organisation.getOrganisationIdentity().getSchemaName().toLowerCase(), sqlFileName);
            ClassPathResource resource = new ClassPathResource(sqlPath);
            try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
                String sql = FileCopyUtils.copyToString(reader);
                
                // Replace parameters in SQL
                if (previousCutoffDateTime != null) {
                    sql = sql.replace(":previousCutoffDateTime", "'" + previousCutoffDateTime + "'");
                }
                if (newCutoffDateTime != null) {
                    sql = sql.replace(":newCutoffDateTime", "'" + newCutoffDateTime + "'");
                }
                if (additionalParams != null) {
                    for (int i = 0; i < additionalParams.size(); i++) {
                        sql = sql.replace(":param" + (i + 1), additionalParams.get(i));
                    }
                }
                
                jdbcTemplate.execute(sql);
            }
        } catch (IOException e) {
            log.error("Error executing SQL file: " + sqlFileName, e);
            throw new RuntimeException("Failed to execute SQL file: " + sqlFileName, e);
        }
    }
}
