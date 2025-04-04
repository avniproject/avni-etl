package org.avniproject.etl.service;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.PostETLConfig;
import org.avniproject.etl.repository.PostETLSyncStatusRepository;
import org.avniproject.etl.util.ObjectMapperSingleton;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.FileCopyUtils;
import org.springframework.util.StringUtils;

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
        this.objectMapper = ObjectMapperSingleton.getObjectMapper();
        this.postETLSyncStatusRepository = postETLSyncStatusRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void executePostETLScripts(Organisation organisation) {
        try {
            String schema = OrgIdentityContextHolder.getDbSchema();
            log.info(String.format("Starting post-ETL SQL script execution for organisation schema: %s", schema));
            
            // Set schema
            log.info(String.format("Setting search path to schema: %s", schema));
            jdbcTemplate.execute(String.format("SET search_path TO %s", schema));

            // Create post_etl_sync_status table if it doesn't exist
            log.info("Creating/verifying post_etl_sync_status table");
            postETLSyncStatusRepository.createTableIfNotExists();
            
            // Load and execute config
            log.info("Loading post-ETL configuration");
            PostETLConfig config = loadConfig(organisation);
            if (config == null) {
                log.info(String.format("No post-ETL config found for organisation: %s, skipping execution", 
                    organisation.getOrganisationIdentity().getSchemaName()));
                return;
            }

            ZonedDateTime previousCutoffDateTime = postETLSyncStatusRepository.getPreviousCutoffDateTime();
            ZonedDateTime newCutoffDateTime = ZonedDateTime.now();
            log.info(String.format("Processing post-ETL scripts from %s to %s", 
                previousCutoffDateTime, newCutoffDateTime));

            // Execute DDL scripts first
            processDDLSqls(organisation, config);

            // Execute DML scripts in order
            processDMLSqls(organisation, config, previousCutoffDateTime, newCutoffDateTime);

            // Update the cutoff datetime for next run
            log.info("Updating cutoff datetime for next run");
            postETLSyncStatusRepository.updateCutoffDateTime(newCutoffDateTime);
            
            log.info("Successfully completed post-ETL SQL script execution");
        } catch (Throwable t) {
            log.error("Error executing post-ETL scripts", t);
            throw new RuntimeException("Failed to execute post-ETL scripts", t);
        } finally {
            // Reset search path
            try {
                log.info("Resetting search path to PUBLIC");
                jdbcTemplate.execute(String.format("SET search_path TO %s", "PUBLIC"));
            } catch (Exception e) {
                log.error("Failed to reset search path in the end", e);
            }
        }
    }

    private void processDDLSqls(Organisation organisation, PostETLConfig config) {
        if (config.getDdl() == null || config.getDdl().isEmpty()) {
            log.info("No DDL scripts to execute");
            return;
        }
        log.info(String.format("Starting DDL execution for %d scripts", config.getDdl().size()));
        // Sort DDL configs based on order before processing
        config.getDdl().stream().sorted(Comparator.comparingInt(PostETLConfig.DDLConfig::getOrder)).forEach(ddl -> {
            try {
                log.info(String.format("Processing DDL for table: %s (order: %d)", ddl.getTable(), ddl.getOrder()));
                if (!tableExists(ddl)) {
                    log.info(String.format("Creating table: %s", ddl.getTable()));
                    executeSqlFile(organisation, ddl.getSql());
                    log.info(String.format("Successfully created table: %s", ddl.getTable()));
                } else {
                    log.info(String.format("Table %s already exists, skipping creation", ddl.getTable()));
                }
            } catch (Exception e) {
                log.error(String.format("Error processing DDL for table: %s", ddl.getTable()), e);
                throw e;
            }
        });
        log.info("Completed DDL execution");
    }

    private void processDMLSqls(Organisation organisation, PostETLConfig config, ZonedDateTime previousCutoffDateTime, ZonedDateTime newCutoffDateTime) {
        if (config.getDml() == null || config.getDml().isEmpty()) {
            log.info("No DML scripts to execute");
            return;
        }
        log.info(String.format("Starting DML execution for %d configurations", config.getDml().size()));
        config.getDml().stream()
            .sorted(Comparator.comparingInt(PostETLConfig.DMLConfig::getOrder))
            .forEach(dmlConfig -> {
                log.info(String.format("Processing DML for table: %s (order: %d)", dmlConfig.getTable(), dmlConfig.getOrder()));
                if (dmlConfig.getSqls() != null) {
                    dmlConfig.getSqls().stream()
                            .sorted(Comparator.comparingInt(PostETLConfig.DMLSourceConfig::getOrder))
                            .forEach(sourceConfig -> {
                                log.info(String.format("Processing source table: %s (order: %d)", sourceConfig.getSourceTableName(), sourceConfig.getOrder()));
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
                                    log.info(String.format("Executing %d update SQL(s)", sourceConfig.getUpdateSqls().size()));
                                    sourceConfig.getUpdateSqls()
                                            .forEach(sql -> {
                                                executeSqlFileWithParams(organisation,
                                                        sql,
                                                        previousCutoffDateTime,
                                                        newCutoffDateTime,
                                                        sourceConfig.getSqlParams());
                                            });
                                }
                                log.info(String.format("Completed processing source table: %s", sourceConfig.getSourceTableName()));
                            });
                }
                log.info(String.format("Completed processing DML for table: %s", dmlConfig.getTable()));
            });
        log.info("Completed DML execution");

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
                    sql = sql.replace(":previousCutoffDateTime", "'" + Timestamp.from(previousCutoffDateTime.toInstant()) + "'");
                }
                if (newCutoffDateTime != null) {
                    sql = sql.replace(":newCutoffDateTime", "'" + Timestamp.from(newCutoffDateTime.toInstant()) + "'");
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
