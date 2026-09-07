package org.avniproject.etl.repository.sql;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.TableMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

public class TransactionalSyncSqlGenerator {
    private final Map<TableMetadata.Type, String> typeMap = new HashMap<>();

    public TransactionalSyncSqlGenerator() {
        typeMap.put(TableMetadata.Type.Household, "individual.sql");
        typeMap.put(TableMetadata.Type.Individual, "individual.sql");
        typeMap.put(TableMetadata.Type.Group, "individual.sql");
        typeMap.put(TableMetadata.Type.Person, "person.sql");
        typeMap.put(TableMetadata.Type.Encounter, "generalEncounter.sql");
        typeMap.put(TableMetadata.Type.ProgramEnrolment, "programEnrolment.sql");
        typeMap.put(TableMetadata.Type.ProgramExit, "programEnrolmentExit.sql");
        typeMap.put(TableMetadata.Type.ProgramEncounter, "programEncounter.sql");
        typeMap.put(TableMetadata.Type.ProgramEncounterCancellation, "programEncounterCancel.sql");
        typeMap.put(TableMetadata.Type.IndividualEncounterCancellation, "generalEncounterCancel.sql");
        typeMap.put(TableMetadata.Type.ManualProgramEnrolmentEligibility, "manualProgramEnrolmentEligibility.sql");
        typeMap.put(TableMetadata.Type.GroupToMember, "groupToMember.sql");
        typeMap.put(TableMetadata.Type.HouseholdToMember, "householdToMember.sql");
        typeMap.put(TableMetadata.Type.Approval, "approval.sql");
        typeMap.put(TableMetadata.Type.Rejection, "rejection.sql");
    }

    private static String toString(String uuid) {
        return uuid == null ? "" : uuid;
    }

    /**
     * An approval or rejection form attaches to any of four mapping shapes, and the decision it produces
     * carries a different entity_type for each (#174). typeMap holds one template per type, so the shape
     * cannot be written into the SQL - it is resolved here and passed in.
     *
     * The mapping mirrors avni-server's EntityApprovalStatusService.getEntityTypeUUID, which is what
     * writes these two columns in the first place.
     */
    private static String approvalEntityType(TableMetadata tableMetadata) {
        boolean hasProgram = tableMetadata.getProgramUuid() != null;
        boolean hasEncounterType = tableMetadata.getEncounterTypeUuid() != null;
        if (hasProgram && hasEncounterType) return "ProgramEncounter";
        if (hasEncounterType) return "Encounter";
        if (hasProgram) return "ProgramEnrolment";
        return "Subject";
    }

    /**
     * entity_type_uuid holds whichever type the decision is against: the encounter type for either
     * encounter shape, the programme for an enrolment, the subject type for a bare subject.
     */
    private static String approvalEntityTypeUuid(TableMetadata tableMetadata) {
        boolean hasProgram = tableMetadata.getProgramUuid() != null;
        boolean hasEncounterType = tableMetadata.getEncounterTypeUuid() != null;
        if (hasEncounterType) return toString(tableMetadata.getEncounterTypeUuid());
        if (hasProgram) return toString(tableMetadata.getProgramUuid());
        return toString(tableMetadata.getSubjectTypeUuid());
    }

    public boolean supports(TableMetadata tableMetadata) {
        return typeMap.containsKey(tableMetadata.getType());
    }

    public String generateSql(TableMetadata tableMetadata, Date startTime, Date endTime) {
        if (supports(tableMetadata)) {
            return getSql(typeMap.get(tableMetadata.getType()), tableMetadata, startTime, endTime);
        }
        throw new RuntimeException("Could not generate sql for" + tableMetadata.getType().toString());
    }

    public String getSql(String fileName, TableMetadata tableMetadata, Date startTime, Date endTime) {
        String template = readSqlFile(fileName);
        String obsColumnName = tableMetadata.getType().equals(TableMetadata.Type.Address) ? "location_properties" : "observations";
        String text = template.replace("${schema_name}", OrgIdentityContextHolder.getDbSchema())
                .replace("${table_name}", tableMetadata.getName())
                .replace("${observations_to_insert_list}", TransactionDataSyncHelper.getListOfObservations(tableMetadata))
                .replace("${concept_maps}", TransactionDataSyncHelper.getConceptMaps(tableMetadata))
                .replace("${cross_join_concept_maps}", "cross join " + TransactionDataSyncHelper.getConceptMapName(tableMetadata))
                .replace("${subject_type_uuid}", toString(tableMetadata.getSubjectTypeUuid()))
                .replace("${selections}", TransactionDataSyncHelper.buildObservationSelection(tableMetadata, obsColumnName))
                .replace("${exit_obs_selections}", TransactionDataSyncHelper.buildObservationSelection(tableMetadata, "program_exit_observations"))
                .replace("${cancel_obs_selections}", TransactionDataSyncHelper.buildObservationSelection(tableMetadata, "cancel_observations"))
                .replace("${encounter_type_uuid}", toString(tableMetadata.getEncounterTypeUuid()))
                .replace("${group_subject_type_uuid}", toString(tableMetadata.getGroupSubjectTypeUuid()))
                .replace("${member_subject_type_uuid}", toString(tableMetadata.getMemberSubjectTypeUuid()))
                .replace("${program_uuid}", toString(tableMetadata.getProgramUuid()))
                .replace("${approval_entity_type}", approvalEntityType(tableMetadata))
                .replace("${approval_entity_type_uuid}", approvalEntityTypeUuid(tableMetadata))
                .replace("${start_time}", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(startTime))
                .replace("${end_time}", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(endTime));
        if (tableMetadata.getType().equals(TableMetadata.Type.Person) && tableMetadata.hasColumn("middle_name")) {
            text = text.replace("${middle_name}", ",middle_name").replace("${middle_name_select}", ", entity.middle_name                                                               as \"middle_name\"");
        } else {
            text = text.replace("${middle_name}", "").replace("${middle_name_select}", "");
        }
        return text;
    }
}
