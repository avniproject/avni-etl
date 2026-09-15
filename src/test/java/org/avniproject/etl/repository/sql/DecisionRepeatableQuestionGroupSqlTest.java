package org.avniproject.etl.repository.sql;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * avniproject/avni-etl#174 - the SQL behind a repeatable question group asked on a decision form.
 *
 * The bug this pins is not the one that took prerelease down; it is the one hiding behind it.
 * getParentTableType() derives the parent from the mapping shape - which uuids are present - and never
 * looked at the form type. An Approval form mapped to a subject type and a visit type has exactly the
 * shape of an encounter form, so it resolved to Encounter: the table would be built with
 * entity_approval_status_id columns and then filled by generalEncounterRepeatableQGObservations.sql
 * reading public.encounter. That builds, runs, and writes the wrong rows - far worse than the crash.
 *
 * So these tests assert the template actually selected, not just that a template was found.
 */
public class DecisionRepeatableQuestionGroupSqlTest {

    @BeforeEach
    public void setUp() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("org174", "org174", "org174"));
    }

    @AfterEach
    public void tearDown() {
        OrgIdentityContextHolder.clear();
    }

    private TableMetadata decisionQuestionGroup(TableMetadata.TableType parent, String programUuid, String encounterTypeUuid) {
        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setType(TableMetadata.Type.RepeatableQuestionGroup);
        tableMetadata.setName("some_table");
        tableMetadata.setSubjectTypeUuid("subject-type-uuid");
        tableMetadata.setProgramUuid(programUuid);
        tableMetadata.setEncounterTypeUuid(encounterTypeUuid);
        tableMetadata.setRepeatableQuestionGroupConceptUuid("rqg-concept-uuid");
        tableMetadata.setDecisionParentTableType(parent);
        return tableMetadata;
    }

    private String sqlFor(TableMetadata tableMetadata) {
        return new RepeatableQuestionGroupSyncSqlGenerator().generateSql(tableMetadata, new Date(0), new Date());
    }

    // The parent the shape alone could not express

    @Test
    public void resolvesADecisionFormAsItsOwnParentNotAsAnEncounter() {
        TableMetadata onAVisitType = decisionQuestionGroup(TableMetadata.TableType.Approval, null, "encounter-type-uuid");

        assertThat("an Approval form on a visit type has an encounter's shape and must not be read as one",
                onAVisitType.getParentTableType(), equalTo(TableMetadata.TableType.Approval));
    }

    @Test
    public void leavesEveryOtherParentToTheMappingShape() {
        TableMetadata encounterGroup = new TableMetadata();
        encounterGroup.setRepeatableQuestionGroupConceptUuid("rqg-concept-uuid");
        encounterGroup.setEncounterTypeUuid("encounter-type-uuid");

        assertThat(encounterGroup.getParentTableType(), equalTo(TableMetadata.TableType.Encounter));
    }

    // Template selection - the silent-wrong-data case

    @Test
    public void readsTheDecisionTableRatherThanTheEncounterTable() {
        String sql = sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, null, "encounter-type-uuid"));

        assertThat(sql, containsString("public.entity_approval_status"));
        assertThat(sql, not(containsString("from public.encounter ")));
    }

    @Test
    public void theApprovalGroupTakesOnlyApprovals() {
        String sql = sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, null, null));

        assertThat(sql, containsString("aps.status = 'Approved'"));
        assertThat(sql, not(containsString("aps.status = 'Rejected'")));
    }

    @Test
    public void theRejectionGroupTakesOnlyRejections() {
        String sql = sqlFor(decisionQuestionGroup(TableMetadata.TableType.Rejection, null, null));

        assertThat(sql, containsString("aps.status = 'Rejected'"));
        assertThat(sql, not(containsString("aps.status = 'Approved'")));
    }

    // The placeholders the repeatable-group generator did not used to substitute

    @Test
    public void leavesNoApprovalPlaceholderUnresolved() {
        String sql = sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, "program-uuid", "encounter-type-uuid"));

        assertThat(sql, not(containsString("${approval_entity_type}")));
        assertThat(sql, not(containsString("${approval_entity_type_uuid}")));
        assertThat(sql, not(containsString("${approval_program_filter}")));
    }

    @Test
    public void resolvesTheMappingShapeTheDecisionWasRecordedAgainst() {
        assertThat(sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, "program-uuid", "encounter-type-uuid")),
                containsString("entity.entity_type = 'ProgramEncounter'"));
        assertThat(sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, "program-uuid", null)),
                containsString("entity.entity_type = 'ProgramEnrolment'"));
        assertThat(sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, null, "encounter-type-uuid")),
                containsString("entity.entity_type = 'Encounter'"));
        assertThat(sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, null, null)),
                containsString("entity.entity_type = 'Subject'"));
    }

    @Test
    public void keepsTwoProgrammesSharingAVisitTypeApart() {
        // Without this the two tables collect each other's decisions - the finding that made the
        // programme filter necessary on the decision tables in the first place.
        String sql = sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, "program-uuid", "encounter-type-uuid"));

        assertThat(sql, containsString("prog.uuid = 'program-uuid'"));
        assertThat(sql, containsString("pe.id = entity.entity_id"));
    }

    @Test
    public void addsNoProgrammeFilterWhereTheShapeIdentifiesItself() {
        assertThat(sqlFor(decisionQuestionGroup(TableMetadata.TableType.Approval, "program-uuid", null)),
                not(containsString("prog.uuid")));
    }
}
