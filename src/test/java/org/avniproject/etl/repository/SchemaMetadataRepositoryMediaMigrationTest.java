package org.avniproject.etl.repository;

import org.avniproject.etl.BaseIntegrationTest;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.diff.AlterColumnType;
import org.avniproject.etl.domain.metadata.diff.Diff;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class SchemaMetadataRepositoryMediaMigrationTest extends BaseIntegrationTest {

    @Autowired
    private SchemaMetadataRepository schemaMetadataRepository;

    @BeforeEach
    public void before() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc"));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/with-media-table-numeric-address-id.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldMigrateMediaTableAddressIdFromNumericToInteger() {
        SchemaMetadata existingSchemaMetadata = schemaMetadataRepository.getExistingSchemaMetadata();
        SchemaMetadata newSchemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();

        List<Diff> changes = newSchemaMetadata.findChanges(existingSchemaMetadata);

        List<Diff> alterColumnTypeDiffs = changes.stream()
                .filter(d -> d instanceof AlterColumnType)
                .toList();
        assertThat(alterColumnTypeDiffs, hasSize(1));
        assertThat(alterColumnTypeDiffs.get(0).getSql(), containsString("alter column \"address_id\" type integer"));

        schemaMetadataRepository.applyChanges(changes);

        String columnType = jdbcTemplate.queryForObject(
                "SELECT data_type FROM information_schema.columns " +
                "WHERE table_schema = 'orgc' AND table_name = 'media' AND column_name = 'address_id'",
                String.class);
        assertThat(columnType, is("integer"));

        List<Integer> addressIds = jdbcTemplate.queryForList(
                "SELECT address_id FROM orgc.media ORDER BY address_id", Integer.class);
        assertThat(addressIds, contains(42, 101));
    }
}
