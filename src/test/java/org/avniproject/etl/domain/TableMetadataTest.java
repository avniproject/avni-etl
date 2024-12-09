package org.avniproject.etl.domain;

import org.avniproject.etl.builder.domain.metadata.TableMetadataBuilder;
import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.metadata.diff.AddColumn;
import org.avniproject.etl.domain.metadata.diff.Diff;
import org.avniproject.etl.domain.metadata.diff.RenameTable;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

public class TableMetadataTest {

    @Test
    public void matches_shouldHandleNullValues() {
        assertThat(new TableMetadata().matches(null), is(false));
    }

    @Test
    public void matches_shouldMatchCorrectlyWithSameTable() {
        assertThat(new TableMetadata().matches(new TableMetadata()), is(true));
    }

    @Test
    public void matches_shouldHandleDifferentTypesOfTables() {
        TableMetadata person = new TableMetadataBuilder().forPerson().build();
        TableMetadata programEnrolment = new TableMetadataBuilder().forProgramEnrolment("1").build();
        TableMetadata anotherProgramEnrolment = new TableMetadataBuilder().forProgramEnrolment("2").build();
        TableMetadata programExit = new TableMetadataBuilder().forProgramExit("1").build();
        TableMetadata anotherProgramExit = new TableMetadataBuilder().forProgramExit("2").build();
        TableMetadata programEncounter = new TableMetadataBuilder().forProgramEncounter("1", "1").build();
        TableMetadata programEncounterCancellation = new TableMetadataBuilder().forProgramEncounterCancellation("1", "1").build();
        TableMetadata anotherProgramProgramEncounter = new TableMetadataBuilder().forProgramEncounter("2", "1").build();
        TableMetadata anotherProgramProgramEncounterCancellation = new TableMetadataBuilder().forProgramEncounterCancellation("2", "1").build();
        TableMetadata addressTable = new TableMetadataBuilder().forAddress().build();
        TableMetadata syncTelemetryTable = new TableMetadataBuilder().forSyncTelemetry().build();
        TableMetadata userTable = new TableMetadataBuilder().forUser().build();

        List<TableMetadata> tables = Arrays.asList(person, programEnrolment, anotherProgramEnrolment,
                programExit, anotherProgramExit, programEncounter, programEncounterCancellation,
                anotherProgramProgramEncounter, anotherProgramProgramEncounterCancellation, addressTable,
                syncTelemetryTable, userTable);

        tables.forEach(firstTable -> {
            tables.forEach(secondTable -> {
                if (firstTable == secondTable) {
                    assertThat(firstTable.matches(secondTable), is(true));
                } else {
                    assertThat(firstTable.matches(secondTable), is(false));
                }
            });
        });
    }

    @Test
    public void shouldRenameTableIfNecessary() {
        TableMetadata oldTable = new TableMetadata();
        oldTable.setName("oldTable");
        TableMetadata newTable = new TableMetadata();
        newTable.setName("newTable");

        List<Diff> changes = newTable.findChanges(oldTable);
        assertThat(changes.size(), is(1));
        assertThat(changes.get(0), instanceOf(RenameTable.class));
    }

    @Test
    public void shouldAddColumnIfMissing() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("dbUser", "schema", "mediaDirectory"));
        TableMetadata oldTable = new TableMetadataBuilder().forPerson().build();
        TableMetadata newTable = new TableMetadataBuilder().forPerson().build();
        newTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("newColumn", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, UUID.randomUUID().toString(), false)));

        List<Diff> changes = newTable.findChanges(oldTable);

        assertThat(changes.size(), is(1));
        Diff diff = changes.get(0);
        assertThat(diff, instanceOf(AddColumn.class));
        assertThat(diff.getSql(), containsString("newColumn"));
    }

    @Test
    public void mergeWith() {
        TableMetadata oldTable = new TableMetadataBuilder().forPerson().build();
        TableMetadata newTable = new TableMetadataBuilder().forPerson().build();

        String column1ConceptUuid = UUID.randomUUID().toString();
        oldTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("concept1", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, column1ConceptUuid, false)));
        String renamedColumn = "renamedColumn";
        newTable.addColumnMetadata(List.of(new ColumnMetadata(new Column(renamedColumn, Column.Type.text), 24, ColumnMetadata.ConceptType.Text, column1ConceptUuid, false)));

        // Voided
        String column2ConceptUuid = UUID.randomUUID().toString();
        oldTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("concept2", Column.Type.text), 25, ColumnMetadata.ConceptType.Text, column2ConceptUuid, false)));
        // new table will not have the voided column, if it is voided, as it is not picked from form element query

        // was previously voided
        String column3ConceptUuid = UUID.randomUUID().toString();
        oldTable.addColumnMetadata(List.of(new ColumnMetadata(new Column(ColumnMetadata.getVoidedName("concept3"), Column.Type.text), 26, ColumnMetadata.ConceptType.Text, column3ConceptUuid, true)));
        // new table will not have the voided column, if it is voided, as it is not picked from form element query

        newTable.mergeWith(oldTable);

        assertEquals(3, newTable.getColumns().size());
        assertNotNull(newTable.getColumnByConceptUuid(column1ConceptUuid));
        ColumnMetadata column2 = newTable.getColumnByConceptUuid(column2ConceptUuid);
        assertNotNull(column2);
        assertTrue(column2.isConceptVoided());

        ColumnMetadata column3 = newTable.getColumnByConceptUuid(column3ConceptUuid);
        assertNotNull(column3);
        assertTrue(column3.isConceptVoided());
    }

    @Test
    public void mergeWith_changeInDataTypeButInDifferentConcept() {
        TableMetadata oldTable = new TableMetadataBuilder().forPerson().build();
        TableMetadata newTable = new TableMetadataBuilder().forPerson().build();

        String conceptName = "concept1";

        String column1ConceptUuid = "uuid-1";
        oldTable.addColumnMetadata(List.of(new ColumnMetadata(new Column(conceptName, Column.Type.text), 24, ColumnMetadata.ConceptType.Text, column1ConceptUuid, false)));
        // new table will not have the voided column, if it is voided, as it is not picked from form element query

        String column2ConceptUuid = "uuid-2";
        newTable.addColumnMetadata(List.of(new ColumnMetadata(new Column(conceptName, Column.Type.numeric), 25, ColumnMetadata.ConceptType.Numeric, column2ConceptUuid, false)));

        newTable.mergeWith(oldTable);

        assertEquals(2, newTable.getColumns().size());
        ColumnMetadata column1 = newTable.getColumnByConceptUuid(column1ConceptUuid);
        assertTrue(column1.isConceptVoided());
        assertNotNull(column1);

        ColumnMetadata column2 = newTable.getColumnByConceptUuid(column2ConceptUuid);
        assertNotNull(column2);
        assertFalse(column2.isConceptVoided());
    }

    @Test
    public void findChangesWhenTheNewColumnNameIsLarge() {
        TableMetadata oldTable = new TableMetadataBuilder().forPerson().build();
        TableMetadata newTable = new TableMetadataBuilder().forPerson().build();

        String conceptUuid = UUID.randomUUID().toString();
        oldTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("Total silt requested by the family members", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, conceptUuid, false)));
        newTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("Total silt requested by the family members – Number of trolle", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, conceptUuid, false)));

        List<Diff> changes = newTable.findChanges(oldTable);
        assertEquals(1, changes.size());
    }

    @Test
    void findBytesLength() {
        String errorColumnName = "Total silt requested by the family members – Number of trolle";
        String columnName = "Total silt requested by the family members – Number of trolleys";
        String expected = "Total silt requested by the family members – Nu (1206887472)";
        assertEquals(65, columnName.getBytes().length);
        assertEquals(63, errorColumnName.getBytes().length);
        assertEquals(62, expected.getBytes().length);
        Column column = new Column(columnName, Column.Type.numeric);
        assertEquals(expected, column.getName());
    }
}
