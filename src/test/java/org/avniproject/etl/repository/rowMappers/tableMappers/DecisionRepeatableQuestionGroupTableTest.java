package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup.ApprovalRepeatableQuestionGroupTable;
import org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup.EncounterRepeatableQuestionGroupTable;
import org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup.RejectionRepeatableQuestionGroupTable;
import org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup.RepeatableQuestionGroupTableFactory;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * avniproject/avni-etl#174 - a repeatable question group asked on an Approval or Rejection form.
 *
 * This case shipped broken and took an organisation's entire ETL run down with it:
 * repeatableQuestionGroups.sql reads parent_table_type straight from f.form_type with no filter, so a
 * decision form containing a repeatable question group reached TableType.valueOf("Approval") and threw.
 * The whole run died, not one table. These tests exist so that cannot recur silently.
 *
 * Naming carries the destructive risk. generateTableName's first argument is only the key into
 * TableNameGenerator.trims - the name is built from the part values plus the suffix. Without a
 * distinguishing suffix a decision group on (subject type, visit type, concept) produces exactly the
 * string EncounterRepeatableQuestionGroupTable produces for the same parts, and CreateTable.getSql()
 * opens with "drop table if exists <name> cascade". A collision does not error; it drops a live table
 * and every view built on it.
 */
public class DecisionRepeatableQuestionGroupTableTest {

    private Map<String, Object> details(String subjectType, String program, String encounterType, String concept) {
        Map<String, Object> tableDetails = new HashMap<>();
        tableDetails.put("subject_type_name", subjectType);
        tableDetails.put("program_name", program);
        tableDetails.put("encounter_type_name", encounterType);
        tableDetails.put("parent_concept_name", concept);
        return tableDetails;
    }

    private List<String> columnNames(Table table) {
        return table.columns().stream().map(Column::getName).collect(Collectors.toList());
    }

    // The factory - the site that threw and killed the run

    @Test
    public void buildsATableForADecisionFormInsteadOfThrowing() {
        Map<String, Object> approval = details("Mother", null, null, "Reasons");
        approval.put("parent_table_type", "Approval");
        Map<String, Object> rejection = details("Mother", null, null, "Reasons");
        rejection.put("parent_table_type", "Rejection");

        assertThat(RepeatableQuestionGroupTableFactory.create(approval),
                instanceOf(ApprovalRepeatableQuestionGroupTable.class));
        assertThat(RepeatableQuestionGroupTableFactory.create(rejection),
                instanceOf(RejectionRepeatableQuestionGroupTable.class));
    }

    // Columns - the child rows hang off the decision, not off the record judged

    @Test
    public void hangsTheRowsOffTheDecision() {
        assertThat(columnNames(new ApprovalRepeatableQuestionGroupTable()),
                hasItems("entity_approval_status_id", "individual_id", "address_id",
                        "repeatable_question_group_index"));
        assertThat(columnNames(new RejectionRepeatableQuestionGroupTable()),
                hasItems("entity_approval_status_id", "individual_id", "address_id",
                        "repeatable_question_group_index"));
    }

    @Test
    public void rejectionCarriesTheSameColumnsAsApproval() {
        assertThat(columnNames(new RejectionRepeatableQuestionGroupTable()),
                equalTo(columnNames(new ApprovalRepeatableQuestionGroupTable())));
    }

    @Test
    public void namesTheParentColumnTheDeduplicationLooksFor() {
        // DuplicateRowDeleteAction substitutes this into ${parentIdColumn}; a missing or wrong entry
        // deletes by a column that is not there.
        assertThat(TableMetadata.qgParentColumnIds.get(TableMetadata.TableType.Approval),
                equalTo("entity_approval_status_id"));
        assertThat(TableMetadata.qgParentColumnIds.get(TableMetadata.TableType.Rejection),
                equalTo("entity_approval_status_id"));
        assertThat(columnNames(new ApprovalRepeatableQuestionGroupTable()),
                hasItem(TableMetadata.qgParentColumnIds.get(TableMetadata.TableType.Approval)));
    }

    // Naming - the destructive case

    @Test
    public void neverCollidesWithTheEncounterQuestionGroupTable() {
        Map<String, Object> sameParts = details("Mother", null, "Home Visit", "Reasons");

        String encounter = new EncounterRepeatableQuestionGroupTable().name(sameParts);
        String approval = new ApprovalRepeatableQuestionGroupTable().name(sameParts);
        String rejection = new RejectionRepeatableQuestionGroupTable().name(sameParts);

        assertThat(approval, not(equalTo(encounter)));
        assertThat(rejection, not(equalTo(encounter)));
        assertThat(approval, not(equalTo(rejection)));
    }

    @Test
    public void namesAllFourMappingShapesDistinctly() {
        List<String> names = List.of(
                new ApprovalRepeatableQuestionGroupTable().name(details("Mother", null, null, "Reasons")),
                new ApprovalRepeatableQuestionGroupTable().name(details("Mother", "ANC", null, "Reasons")),
                new ApprovalRepeatableQuestionGroupTable().name(details("Mother", null, "Home Visit", "Reasons")),
                new ApprovalRepeatableQuestionGroupTable().name(details("Mother", "ANC", "Home Visit", "Reasons")));

        assertThat(names.stream().distinct().count(), equalTo(4L));
    }

    @Test
    public void keepsTwoProgrammesSharingAVisitTypeApart() {
        // The same pair that made the decision tables collect each other's rows before the programme
        // filter was added.
        String anc = new ApprovalRepeatableQuestionGroupTable().name(details("Mother", "ANC", "Home Visit", "Reasons"));
        String pnc = new ApprovalRepeatableQuestionGroupTable().name(details("Mother", "PNC", "Home Visit", "Reasons"));

        assertThat(anc, not(equalTo(pnc)));
    }

    @Test
    public void survivesAbsentPartsRatherThanThrowing() {
        assertThat(new ApprovalRepeatableQuestionGroupTable().name(details("Mother", null, null, "Reasons")),
                not(emptyOrNullString()));
    }

    /**
     * ApprovalTable has this guard; these two did not, and they carry a fourth part - the question group
     * concept - so they run longer than any existing decision table name. Postgres truncates at 63, and a
     * truncated name that lands on an existing table is dropped rather than rejected.
     */
    @Test
    public void staysWithinPostgresTableNameLimit() {
        Map<String, Object> longNames = details(
                "An extremely long subject type name that runs past sixty three characters on its own",
                "An extremely long programme name that also runs well past sixty three characters",
                "An extremely long encounter type name that does the same again",
                "An extremely long repeatable question group concept name as well");

        assertThat("postgres truncates at 63 characters",
                new ApprovalRepeatableQuestionGroupTable().name(longNames).length(), lessThanOrEqualTo(63));
        assertThat("postgres truncates at 63 characters",
                new RejectionRepeatableQuestionGroupTable().name(longNames).length(), lessThanOrEqualTo(63));
    }
}
