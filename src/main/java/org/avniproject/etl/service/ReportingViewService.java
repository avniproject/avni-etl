package org.avniproject.etl.service;

import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.repository.ReportingViewRepository;
import org.avniproject.etl.repository.sync.MultiSelectViewSyncAction;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class ReportingViewService {
    private final ReportingViewRepository reportingViewRepository;
    private final MultiSelectViewSyncAction multiSelectViewSyncAction;
    private final JdbcTemplate jdbcTemplate;
    private static final Logger log = Logger.getLogger(ReportingViewService.class);

    @Autowired
    public ReportingViewService(ReportingViewRepository reportingViewRepository,
                              MultiSelectViewSyncAction multiSelectViewSyncAction,
                              JdbcTemplate jdbcTemplate) {
        this.reportingViewRepository = reportingViewRepository;
        this.multiSelectViewSyncAction = multiSelectViewSyncAction;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void processViews(Organisation organisation) {
        OrganisationIdentity organisationIdentity = organisation.getOrganisationIdentity();
        reportingViewRepository.createOrReplaceView(organisationIdentity);
        log.info(String.format("Created reporting views for %s", organisationIdentity));
        multiSelectViewSyncAction.createMultiselectViews(organisationIdentity, organisation.getSchemaMetadata());
        log.info(String.format("Created multiselect views for %s", organisationIdentity));
        
        // Grant permissions on all tables and views in the schema
        grantSchemaPermissions(organisationIdentity);
    }
    
    private void grantSchemaPermissions(OrganisationIdentity organisationIdentity) {
        String schemaName = organisationIdentity.getSchemaName();
        String dbUser = organisationIdentity.getDbUser();
        
        try {
            jdbcTemplate.execute("RESET ROLE");
            
            jdbcTemplate.execute(String.format("GRANT ALL ON ALL TABLES IN SCHEMA \"%s\" TO \"%s\"", schemaName, dbUser));
            
            jdbcTemplate.execute(String.format("GRANT ALL ON ALL SEQUENCES IN SCHEMA \"%s\" TO \"%s\"", schemaName, dbUser));
            
            log.info(String.format("Granted permissions on all objects in schema %s to user %s", schemaName, dbUser));
        } catch (Exception e) {
            log.warn("Failed to grant schema permissions for " + organisationIdentity + ": " + e.getMessage());
        }
    }
}
