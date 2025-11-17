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
    }

    private static String toString(String uuid) {
        return uuid == null ? "" : uuid;
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
        String text = template.replace("${schema_name}", TransactionDataSyncHelper.wrapInQuotes(OrgIdentityContextHolder.getDbSchema()))
                .replace("${table_name}", TransactionDataSyncHelper.wrapInQuotes(tableMetadata.getName()))
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
