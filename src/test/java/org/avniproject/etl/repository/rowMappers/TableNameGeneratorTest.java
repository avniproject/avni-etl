package org.avniproject.etl.repository.rowMappers;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.avniproject.etl.repository.rowMappers.TableNameGenerator.EncounterRepeatableQuestionGroup;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class TableNameGeneratorTest {

    @Test
    public void shouldConvertsSubjectTypeToLowerCaseAndReplacesSpacesWithUnderscore() {
        String subjectTypeTable = new TableNameGenerator().generateName(List.of("Dam"), "IndividualProfile", null);
        assertThat(subjectTypeTable, equalTo("dam"));

        subjectTypeTable = new TableNameGenerator().generateName(List.of("People Survey"), "IndividualProfile", null);
        assertThat(subjectTypeTable, equalTo("people_survey"));
    }

    @Test
    public void shouldNotAppendDisambiguatorWhenNameFitsWithoutTrimming() {
        // Short combined name (< 63 chars) is returned as-is regardless of UUID
        String name = new TableNameGenerator().generateName(
                List.of("Outreach", "Visit"), "Encounter", null,
                "0e1bc142-2841-4b8e-bf45-0e322ed0563d");
        assertThat(name, equalTo("outreach_visit"));
    }

    @Test
    public void shouldAppendUuidHashSuffixWhenTruncationKicksIn() {
        // Untrimmed combined name > 63 chars -> trim fires -> hash suffix appended
        List<String> entities = List.of(
                "Outreach",
                "Outreach Location Specific Details",
                "3 log jin se milein - Unka Naam, Gender and Phone Number - Village");
        String name = new TableNameGenerator().generateName(
                entities, EncounterRepeatableQuestionGroup, null,
                "0e1bc142-2841-4b8e-bf45-0e322ed0563d");
        assertThat(name, equalTo("outrea_outreach_location_sp_3_log_jin_se_milein__0e1bc1"));
    }

    @Test
    public void shouldDifferentiateCollidingTrimmedNamesByDisambiguatorUuid() {
        // Two RQG concepts with different "tail" but identical first-20 chars
        List<String> villageEntities = List.of(
                "Outreach", "Outreach Location Specific Details",
                "3 log jin se milein - Unka Naam, Gender and Phone Number - Village");
        List<String> otherSpaceEntities = List.of(
                "Outreach", "Outreach Location Specific Details",
                "3 log jin se milein - Unka Naam, Gender and Phone Number - Other Space");

        String village = new TableNameGenerator().generateName(
                villageEntities, EncounterRepeatableQuestionGroup, null,
                "0e1bc142-2841-4b8e-bf45-0e322ed0563d");
        String otherSpace = new TableNameGenerator().generateName(
                otherSpaceEntities, EncounterRepeatableQuestionGroup, null,
                "244c6850-0326-4dda-9e73-a7726805c1d0");

        // Both share a prefix (truncation collapses the tail) but the hash suffix differs
        String sharedPrefix = "outrea_outreach_location_sp_3_log_jin_se_milein_";
        assertThat(village, startsWith(sharedPrefix));
        assertThat(otherSpace, startsWith(sharedPrefix));
        assertThat(village, not(equalTo(otherSpace)));
    }

    @Test
    public void shouldFallBackToLegacyTrimmedNameWhenNoUuidProvided() {
        List<String> entities = List.of(
                "Outreach", "Outreach Location Specific Details",
                "3 log jin se milein - Unka Naam, Gender and Phone Number - Village");
        // Three-arg call (no UUID) preserves pre-existing behavior so callers that don't
        // yet pass a UUID don't accidentally rename existing tables.
        String legacy = new TableNameGenerator().generateName(
                entities, EncounterRepeatableQuestionGroup, null);
        assertThat(legacy, equalTo("outrea_outreach_location_sp_3_log_jin_se_milein_"));
    }

    @Test
    public void shouldStayWithinPostgresIdentifierLimit() {
        // Even the deepest table type (ProgramEncounterRQG: trims 6,6,20,20 -> ~54 chars)
        // plus "_" + 6-char hash must fit in 63.
        List<String> entities = List.of(
                "Subject Type Name Some Long",
                "Some Program Name Likewise Long",
                "Some Encounter Type Name Likewise Long",
                "RQG Concept Name Long Enough To Trigger Truncation Easily");
        String name = new TableNameGenerator().generateName(
                entities,
                TableNameGenerator.ProgramEncounterRepeatableQuestionGroup,
                null,
                "abcdef12-3456-7890-abcd-ef1234567890");
        assertThat(name.length() <= 63, equalTo(true));
    }
}
