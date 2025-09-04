package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TableTest {
    @Test
    void shouldGetAllColumns() {
        assertThat(new SubjectTable().columns().size(), is(15));
        assertThat(new PersonTable(false).columns().size(), is(18));

        assertThat(new EncounterTable().columns().size(), is(20));
        assertThat(new ProgramEncounterCancellationTable().columns().size(), is(21));
        assertThat(new ProgramEncounterTable().columns().size(), is(21));

        assertThat(new ProgramEnrolmentTable().columns().size(), is(16));
        assertThat(new ProgramExitTable().columns().size(), is(16));

        assertThat(new SyncTelemetryTable().columns().size(), is(18));
        assertThat(new UserTable().columns().size(), is(13));
    }
}
