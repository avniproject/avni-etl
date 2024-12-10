package org.avniproject.etl.repository;

import org.avniproject.etl.BaseIntegrationTest;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.*;
import org.avniproject.etl.domain.metadata.diff.Diff;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class SchemaMetadataRepositoryTest extends BaseIntegrationTest {

    @Autowired
    private SchemaMetadataRepository schemaMetadataRepository;

    @BeforeEach
    public void before() {
        OrganisationIdentity orgb = OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc");
        OrgIdentityContextHolder.setContext(orgb);
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetAllTablesForAnOrganisation() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        assertThat(schemaMetadata.getTableMetadata().size(), is(16));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetDecisionConcepts() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        TableMetadata growthMonitoringEncounterTable = schemaMetadata.getTableMetadata().stream().filter(tableMetadata1 -> tableMetadata1.getName().equals("person_nutrition_growth_monitoring")).findFirst().get();
        List<ColumnMetadata> decisionColumns = growthMonitoringEncounterTable.getNonDefaultColumnMetadataList().stream().filter(c -> !c.getColumn().isSyncAttributeColumn()).collect(Collectors.toList());
        assertThat(decisionColumns.size(), is(1));
        ColumnMetadata decisionConceptColumn = decisionColumns.get(0);
        assertThat(decisionConceptColumn.getName(), is("Goat Color"));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetAddressTable() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        Optional<TableMetadata> addressTableOptional = schemaMetadata.getTableMetadata().stream().filter(tableMetadata1 -> tableMetadata1.getName().equals("address")).findFirst();

        assertThat(addressTableOptional.isPresent(), is(true));

        TableMetadata addressTable = addressTableOptional.get();
        assertThat(addressTable.getColumns().size(), is(17));
        assertThat(addressTable.getColumns().stream().anyMatch(column -> column.getName().equals("District")), is(true));
        assertThat(addressTable.getColumns().stream().anyMatch(column -> column.getName().equals("District id")), is(true));
        assertThat(addressTable.getColumns().stream().anyMatch(column -> column.getName().equals("Block")), is(true));
        assertThat(addressTable.getColumns().stream().anyMatch(column -> column.getName().equals("Block id")), is(true));
        assertThat(addressTable.getColumns().stream().anyMatch(column -> column.getName().equals("Gram Panchayat")), is(true));
        assertThat(addressTable.getColumns().stream().anyMatch(column -> column.getName().equals("Gram Panchayat id")), is(true));
        assertThat(addressTable.getColumns().stream().anyMatch(column -> column.getName().equals("gps_coordinates")), is(true));
        assertThat(addressTable.getColumns().stream().anyMatch(column -> column.getName().equals("Extra location info")), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetMediaTable() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        Optional<TableMetadata> mediaTable = schemaMetadata.getTableMetadata().stream().filter(tableMetadata1 -> tableMetadata1.getName().equals("media")).findFirst();

        assertThat(mediaTable.isPresent(), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetMediaAnalysisTable() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        Optional<TableMetadata> mediaAnalysis = schemaMetadata.getTableMetadata().stream().filter(tableMetadata1 -> tableMetadata1.getName().equals("media_analysis")).findFirst();

        assertThat(mediaAnalysis.isPresent(), is(true));
    }


    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetSyncTelemetryTable() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        Optional<TableMetadata> syncTelemetryTable = schemaMetadata.getTableMetadata().stream().filter(tableMetadata1 -> tableMetadata1.getName().equals("sync_telemetry")).findFirst();

        assertThat(syncTelemetryTable.isPresent(), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetUserTable() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        Optional<TableMetadata> userTable = schemaMetadata.getTableMetadata().stream().filter(tableMetadata1 -> tableMetadata1.getName().equals("users")).findFirst();

        assertThat(userTable.isPresent(), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldNotHaveAnySchemaDiffsIfSchemaMetadataHasBeenSavedOnce() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        schemaMetadataRepository.save(schemaMetadata);
        List<Diff> changes = schemaMetadata.findChanges(schemaMetadataRepository.getNewSchemaMetadata());
        assertThat(changes.size(), is(0));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/with-metadata.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetIndexMetadataWhenItExists() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getExistingSchemaMetadata();

        List<IndexMetadata> personTableIndexMetadata = schemaMetadata.getTableMetadata().stream().filter(tableMetadata -> tableMetadata.getName().equals("person")).findFirst().get().getIndexMetadataList();
        assertThat(personTableIndexMetadata, hasSize(2));
        assertThat(personTableIndexMetadata, hasItem(hasProperty("name", equalTo("orgc_12344_idx"))));
        IndexMetadata idIndex = personTableIndexMetadata.stream().filter(indexMetadata -> indexMetadata.getName().equals("orgc_12344_idx")).findFirst().get();
        assertThat(idIndex.matches(new IndexMetadata(new ColumnMetadata(new Column("id", Column.Type.numeric), null, null, null, false))), equalTo(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = {"/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldGetIndicesWhenGettingNewSchemaMetadata() {
        SchemaMetadata schemaMetadata = schemaMetadataRepository.getNewSchemaMetadata();
        List<IndexMetadata> personTableIndexMetadata = schemaMetadata.getTableMetadata().stream().filter(tableMetadata -> tableMetadata.getName().equals("person")).findFirst().get().getIndexMetadataList();
        assertThat(personTableIndexMetadata, hasSize(9));
    }
}
