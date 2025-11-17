package org.avniproject.etl.service;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.diff.Diff;
import org.avniproject.etl.repository.OrganisationRepository;
import org.avniproject.etl.repository.SchemaMetadataRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class SchemaMigrationService {
    private final SchemaMetadataRepository schemaMetadataRepository;
    private final OrganisationRepository organisationRepository;
    private static final Logger log = Logger.getLogger(SchemaMigrationService.class);

    @Autowired
    public SchemaMigrationService(SchemaMetadataRepository schemaMetadataRepository, OrganisationRepository organisationRepository) {
        this.schemaMetadataRepository = schemaMetadataRepository;
        this.organisationRepository = organisationRepository;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Organisation migrate(Organisation organisation) {
        OrganisationIdentity organisationIdentity = organisation.getOrganisationIdentity();
        ensureSchemaExists(organisationIdentity);

        log.info(String.format("Migrating schema for organisation: %s", organisationIdentity));
        SchemaMetadata newSchemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        SchemaMetadata oldSchemaMetadata = organisation.getSchemaMetadata();

        List<Diff> changes = newSchemaMetadata.findChanges(oldSchemaMetadata);
        
        schemaMetadataRepository.applyChanges(changes);

        organisation.applyNewSchema(newSchemaMetadata);

        schemaMetadataRepository.save(organisation.getSchemaMetadata());

        return organisation;
    }
    
    // Utility method for debugging - extracts table name from CREATE TABLE SQL
    // Currently unused but kept for future debugging needs
    private String extractTableNameFromSQL(String sql) {
        // Extract table name from CREATE TABLE "schema"."tablename"
        String pattern = "CREATE TABLE.*?\\.\"([^\"]+)\"";
        java.util.regex.Pattern p = java.util.regex.Pattern.compile(pattern);
        java.util.regex.Matcher m = p.matcher(sql);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    private void ensureSchemaExists(OrganisationIdentity organisationIdentity) {
        organisationRepository.createDBUser(organisationIdentity.getDbUser(), "password");
        List<String> dbUsers = organisationIdentity.getUsersWithSchemaAccess();
        dbUsers.forEach(user -> organisationRepository.createImplementationSchema(organisationIdentity.getSchemaName(), user));
    }
}
