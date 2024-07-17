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
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        newTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("newColumn", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, UUID.randomUUID().toString())));

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

        String conceptUuid = UUID.randomUUID().toString();
        oldTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("oldColumn", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, conceptUuid)));
        newTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("renamedColumn", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, conceptUuid)));

        newTable.mergeWith(oldTable);

        assertEquals(1, newTable.getColumns().size());
        assertEquals("renamedColumn", newTable.getColumns().get(0).getName());
    }

    @Test
    public void findChangesWhenTheNewColumnNameIsLarge() {
        TableMetadata oldTable = new TableMetadataBuilder().forPerson().build();
        TableMetadata newTable = new TableMetadataBuilder().forPerson().build();

        String conceptUuid = UUID.randomUUID().toString();
        oldTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("Total silt requested by the family members", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, conceptUuid)));
        newTable.addColumnMetadata(List.of(new ColumnMetadata(new Column("Total silt requested by the family members – Number of trolle", Column.Type.text), 24, ColumnMetadata.ConceptType.Text, conceptUuid)));

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
