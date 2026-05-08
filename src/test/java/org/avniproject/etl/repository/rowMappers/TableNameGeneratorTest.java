package org.avniproject.etl.repository.rowMappers;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TableNameGeneratorTest {

    @Test
    public void shouldConvertsSubjectTypeToLowerCaseAndReplacesSpacesWithUnderscore() {
        String subjectTypeTable = new TableNameGenerator().generateName(List.of("Dam"), "IndividualProfile", null);
        assertThat(subjectTypeTable, equalTo("dam"));

        subjectTypeTable = new TableNameGenerator().generateName(List.of("People Survey"), "IndividualProfile", null);
        assertThat(subjectTypeTable, equalTo("people_survey"));
    }
}
