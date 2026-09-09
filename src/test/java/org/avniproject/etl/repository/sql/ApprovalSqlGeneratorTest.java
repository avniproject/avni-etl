package org.avniproject.etl.repository.sql;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * avniproject/avni-etl#174 - the SQL behind the approval and rejection reporting tables.
 *
 * Two things are pinned here that the templates alone cannot express. The mapping shape decides which
 * entity_type the decision carries, and typeMap holds one template per type - so the shape has to be
 * resolved in Java. And the generated SQL has to reference the two aliases
 * TransactionDataSyncHelper.buildObservationSelection emits: entity for form answers, ind for the
 * sync-attribute columns every non-subject table is given.
 */
public class ApprovalSqlGeneratorTest {

    // getSql reads the schema name off a thread-local, so the org context has to be set as the ETL run
    // would set it, and cleared afterwards so it cannot leak into another test on the same thread.
    @BeforeEach
    public void setUp() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("org174", "org174", "org174"));
    }

    @AfterEach
    public void tearDown() {
        OrgIdentityContextHolder.clear();
    }

    private TableMetadata tableMetadata(TableMetadata.Type type, String programUuid, String encounterTypeUuid) {
        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setType(type);
        tableMetadata.setName("some_table");
        tableMetadata.setSubjectTypeUuid("subject-type-uuid");
        tableMetadata.setProgramUuid(programUuid);
        tableMetadata.setEncounterTypeUuid(encounterTypeUuid);
        return tableMetadata;
    }

    private String sqlFor(TableMetadata tableMetadata) {
        return new TransactionalSyncSqlGenerator()
                .getSql(tableMetadata.getType() == TableMetadata.Type.Rejection ? "rejection.sql" : "approval.sql",
                        tableMetadata, new java.util.Date(0), new java.util.Date());
    }

    // Registration - without this the sync throws at runtime

    @Test
    public void supportsBothNewTypes() {
        TransactionalSyncSqlGenerator generator = new TransactionalSyncSqlGenerator();

        assertThat(generator.supports(tableMetadata(TableMetadata.Type.Approval, null, null)), is(true));
        assertThat(generator.supports(tableMetadata(TableMetadata.Type.Rejection, null, null)), is(true));
    }

    // AC3 - each table holds only its own decisions

    @Test
    public void theApprovalTableTakesOnlyApprovals() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, null, null));

        assertThat(sql, containsString("aps.status = 'Approved'"));
        assertThat(sql, not(containsString("aps.status = 'Rejected'")));
    }

    @Test
    public void theRejectionTableTakesOnlyRejections() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Rejection, null, null));

        assertThat(sql, containsString("aps.status = 'Rejected'"));
        assertThat(sql, not(containsString("aps.status = 'Approved'")));
    }

    // The four mapping shapes, mirroring avni-server's getEntityTypeUUID

    @Test
    public void aSubjectOnlyMappingReadsSubjectDecisions() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, null, null));

        assertThat(sql, containsString("entity.entity_type = 'Subject'"));
        assertThat(sql, containsString("entity.entity_type_uuid = 'subject-type-uuid'"));
    }

    @Test
    public void aProgrammeMappingReadsEnrolmentDecisions() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, "programme-uuid", null));

        assertThat(sql, containsString("entity.entity_type = 'ProgramEnrolment'"));
        assertThat(sql, containsString("entity.entity_type_uuid = 'programme-uuid'"));
    }

    @Test
    public void anEncounterMappingReadsEncounterDecisions() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, null, "encounter-type-uuid"));

        assertThat(sql, containsString("entity.entity_type = 'Encounter'"));
        assertThat(sql, containsString("entity.entity_type_uuid = 'encounter-type-uuid'"));
    }

    @Test
    public void aProgrammeVisitMappingReadsProgrammeEncounterDecisions() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, "programme-uuid", "encounter-type-uuid"));

        assertThat(sql, containsString("entity.entity_type = 'ProgramEncounter'"));
        assertThat("the encounter type is the more specific of the two",
                sql, containsString("entity.entity_type_uuid = 'encounter-type-uuid'"));
    }

    /**
     * No placeholder may survive into the executed SQL. An unsubstituted ${...} is a silent failure -
     * postgres would reject it, and the org's sync dies rather than the table being wrong.
     */
    @Test
    public void leavesNoUnsubstitutedPlaceholders() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, "programme-uuid", "encounter-type-uuid"));

        assertThat(sql, not(containsString("${")));
    }

    @Test
    public void leavesNoUnsubstitutedPlaceholdersOnAnyShape() {
        assertThat(sqlFor(tableMetadata(TableMetadata.Type.Approval, null, null)), not(containsString("${")));
        assertThat(sqlFor(tableMetadata(TableMetadata.Type.Approval, "programme-uuid", null)), not(containsString("${")));
        assertThat(sqlFor(tableMetadata(TableMetadata.Type.Rejection, null, "encounter-type-uuid")), not(containsString("${")));
    }

    /**
     * Two programmes sharing one visit type each get the other's decisions without this.
     *
     * A decision on a programme visit records entity_type ProgramEncounter and the visit type in
     * entity_type_uuid; the programme is not on the row and never has been. So (Mother, ANC, Home Visit)
     * and (Mother, PNC, Home Visit) generate a byte-identical filter, and one ANC approval lands in the
     * PNC table too - a report counting PNC approvals counts ANC ones as well, with no error anywhere.
     * 52 such subject-type/visit-type pairs exist across 30 organisations today; none has approval
     * switched on yet, so this is latent rather than live.
     */
    @Test
    public void aProgrammeVisitMappingTakesOnlyItsOwnProgrammesDecisions() {
        String anc = sqlFor(tableMetadata(TableMetadata.Type.Approval, "anc-uuid", "home-visit-uuid"));
        String pnc = sqlFor(tableMetadata(TableMetadata.Type.Approval, "pnc-uuid", "home-visit-uuid"));

        assertThat(anc, containsString("prog.uuid = 'anc-uuid'"));
        assertThat(pnc, containsString("prog.uuid = 'pnc-uuid'"));
        assertThat("the two tables must not generate the same filter",
                anc.replace("anc-uuid", "X"), not(is(pnc.replace("pnc-uuid", "X"))));
    }

    /**
     * The programme is recovered by following the decision to the record it judged, which is the only
     * place it is written down. entity_id points at the program_encounter.
     */
    @Test
    public void findsTheProgrammeByFollowingTheRecordThatWasJudged() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, "anc-uuid", "home-visit-uuid"));

        assertThat(sql, containsString("pe.id = entity.entity_id"));
        assertThat(sql, containsString("public.program_enrolment pen"));
        assertThat(sql, containsString("public.program prog"));
        assertThat("EXISTS, not a join - this template promises one row per decision",
                sql, containsString("AND EXISTS (SELECT 1 FROM public.program_encounter pe"));
    }

    /**
     * The other three shapes identify themselves. entity_type_uuid is the subject type for a bare
     * subject and the programme itself for an enrolment, and a plain visit has no programme to lose - so
     * the filter must be empty rather than excluding every row.
     */
    @Test
    public void theOtherThreeShapesGetNoProgrammeFilter() {
        assertThat(sqlFor(tableMetadata(TableMetadata.Type.Approval, null, null)), not(containsString("prog.uuid")));
        assertThat(sqlFor(tableMetadata(TableMetadata.Type.Approval, "programme-uuid", null)), not(containsString("prog.uuid")));
        assertThat(sqlFor(tableMetadata(TableMetadata.Type.Approval, null, "encounter-type-uuid")), not(containsString("prog.uuid")));
    }

    @Test
    public void theRejectionTemplateFiltersByProgrammeToo() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Rejection, "anc-uuid", "home-visit-uuid"));

        assertThat(sql, containsString("prog.uuid = 'anc-uuid'"));
        assertThat(sql, containsString("aps.status = 'Rejected'"));
    }

    /**
     * The regression test for the missing alias. buildObservationSelection emits ind.observations for
     * sync-attribute columns, which SchemaMetadataRepository adds to every non-subject table - so a
     * template sourcing only entity_approval_status would generate SQL referencing an undefined alias
     * and fail for every organisation that has a sync attribute configured.
     */
    @Test
    public void joinsTheIndividualSoSyncAttributeColumnsResolve() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, null, null));

        assertThat(sql, containsString("public.entity_approval_status entity"));
        assertThat(sql, containsString("public.individual ind"));
    }

    /**
     * AC6 - a report can join a decision back to the record it judged.
     */
    @Test
    public void carriesTheJudgedRecordsIdThrough() {
        String sql = sqlFor(tableMetadata(TableMetadata.Type.Approval, null, null));

        assertThat(sql, containsString("entity.entity_id"));
    }
}
