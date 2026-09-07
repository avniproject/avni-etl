package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * avniproject/avni-etl#174 - the reporting tables for approval and rejection form answers.
 *
 * Naming carries most of the risk here, for a reason that is easy to miss: the first argument to
 * generateTableName is NOT part of the name. TableNameGenerator uses it only as the key into its trims
 * map, and builds the name from the part values plus the suffix. So a table named from
 * (subject_type_name, encounter_type_name) with a null suffix produces exactly the string EncounterTable
 * already produces for the same pair - and CreateTable.getSql() opens with
 * "drop table if exists <name> cascade". A collision does not error; it drops a live org's encounter
 * table and every view built on it.
 *
 * These are also the first form types with a variable mapping shape - subject only, +programme,
 * +encounter type, or both - so the name has to stay unique across all four and survive absent parts.
 */
public class ApprovalRejectionTableTest {

    private Map<String, Object> details(String subjectType, String program, String encounterType) {
        Map<String, Object> tableDetails = new HashMap<>();
        tableDetails.put("subject_type_name", subjectType);
        tableDetails.put("program_name", program);
        tableDetails.put("encounter_type_name", encounterType);
        return tableDetails;
    }

    // Columns - AC2

    @Test
    public void carriesTheReportingColumnsTheStoryAsksFor() {
        List<String> names = new ApprovalTable().columns().stream()
                .map(Column::getName).collect(Collectors.toList());

        assertThat(names, hasItems("id", "individual_id", "address_id", "entity_id", "entity_type",
                "entity_type_uuid", "approval_status", "status_date_time", "approval_status_comment",
                "auto_approved"));
    }

    @Test
    public void indexesTheColumnsAReportWillJoinOn() {
        Map<String, Column> byName = new ApprovalTable().columns().stream()
                .collect(Collectors.toMap(Column::getName, c -> c, (a, b) -> a));

        assertThat(byName.get("individual_id").getType(), equalTo(Column.Type.integer));
        assertThat(byName.get("entity_id").getType(), equalTo(Column.Type.integer));
        assertThat(byName.get("address_id").getType(), equalTo(Column.Type.integer));
    }

    @Test
    public void rejectionCarriesTheSameColumnsAsApproval() {
        assertThat(new RejectionTable().columns().stream().map(Column::getName).collect(Collectors.toList()),
                equalTo(new ApprovalTable().columns().stream().map(Column::getName).collect(Collectors.toList())));
    }

    // Naming - the four shapes

    @Test
    public void namesAllFourMappingShapesDistinctly() {
        ApprovalTable table = new ApprovalTable();
        List<String> names = List.of(
                table.name(details("Mother", null, null)),
                table.name(details("Mother", "ANC Programme", null)),
                table.name(details("Mother", null, "ANC Visit")),
                table.name(details("Mother", "ANC Programme", "ANC Visit")));

        assertThat("all four shapes must be distinguishable: " + names,
                names.stream().distinct().count(), equalTo(4L));
    }

    /**
     * The subject-only shape is the commonest configuration and the one a fixed part list would have
     * killed - buildProperTableName calls String::toLowerCase on every part, so a null encounter type
     * name aborts the whole org's schema build.
     */
    @Test
    public void namesTheSubjectOnlyShapeWithoutFallingOverOnTheAbsentParts() {
        String name = new ApprovalTable().name(details("Mother", null, null));

        assertThat(name, notNullValue());
        assertThat(name, not(containsString("null")));
    }

    /**
     * The regression test for the drop-table collision. If Approval carried a null suffix and only the
     * subject/encounter parts, this would equal the encounter table's own name.
     */
    @Test
    public void neverCollidesWithTheTableOfAnyOtherFormTypeForTheSameTypes() {
        Map<String, Object> mapping = details("Mother", "ANC Programme", "ANC Visit");

        String approval = new ApprovalTable().name(mapping);
        String rejection = new RejectionTable().name(mapping);

        List<String> others = List.of(
                new EncounterTable().name(mapping),
                new EncounterCancellationTable().name(mapping),
                new ProgramEncounterTable().name(mapping),
                new ProgramEncounterCancellationTable().name(mapping),
                new ProgramEnrolmentTable().name(mapping),
                new ProgramExitTable().name(mapping),
                new SubjectTable().name(mapping));

        assertThat("approval must not resolve to an existing table's name: " + approval + " vs " + others,
                others, not(hasItem(approval)));
        assertThat("rejection must not resolve to an existing table's name: " + rejection + " vs " + others,
                others, not(hasItem(rejection)));
        assertThat("approval and rejection must differ from each other", approval, not(equalTo(rejection)));
    }

    /**
     * Long names fall through to getTrimmedTableName, which reads trims.get(tableType) - a missing key
     * is an NPE, not a truncation. EncounterCancellationTable passes "Encounter" rather than its own
     * name precisely so that lookup lands; these types need their own registered entries.
     */
    @Test
    public void truncatesRatherThanThrowingOnVeryLongTypeNames() {
        Map<String, Object> longNames = details(
                "An extremely long subject type name used by a real organisation",
                "An extremely long programme name that also runs well past sixty three characters",
                "An extremely long encounter type name that does the same again");

        String name = new ApprovalTable().name(longNames);

        assertThat(name, notNullValue());
        assertThat("postgres truncates at 63 characters", name.length(), lessThanOrEqualTo(63));
    }

    // The enum

    @Test
    public void bothTypesAreKnownTableTypes() {
        assertThat(TableMetadata.Type.valueOf("Approval"), equalTo(TableMetadata.Type.Approval));
        assertThat(TableMetadata.Type.valueOf("Rejection"), equalTo(TableMetadata.Type.Rejection));
    }

    /**
     * TableType is the list of parent entity types a repeatable question group can hang off, not a list
     * of kinds of table. RepeatableQuestionGroupTableFactory switches over it as an exhaustive switch
     * expression with no default, so adding a constant is a compile error - and qgParentColumnIds is
     * keyed by it, so an entry there would be unreachable. This guard exists so the next person to read
     * the story the way it is written gets a red test rather than a puzzling compile failure.
     */
    @Test
    public void tableTypeStillEnumeratesOnlyRepeatableQuestionGroupParents() {
        assertThat(List.of(TableMetadata.TableType.values()),
                containsInAnyOrder(TableMetadata.TableType.IndividualProfile,
                        TableMetadata.TableType.Encounter,
                        TableMetadata.TableType.ProgramEnrolment,
                        TableMetadata.TableType.ProgramEncounter));
    }
}
