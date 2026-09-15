package org.avniproject.etl.repository.sql;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.TableMetadata;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

public class RepeatableQuestionGroupSyncSqlGenerator {
    private static final Map<TableMetadata.TableType, String> parentTypeFileMap = new HashMap<>() {{
        this.put(TableMetadata.TableType.IndividualProfile, "subjectRepeatableQGObservations.sql");
        this.put(TableMetadata.TableType.Encounter, "generalEncounterRepeatableQGObservations.sql");
        this.put(TableMetadata.TableType.ProgramEnrolment, "programEnrolmentRepeatableQGObservations.sql");
        this.put(TableMetadata.TableType.ProgramEncounter, "programEncounterRepeatableQGObservations.sql");
        // #174 - a decision form can carry a repeatable question group like any other form.
        this.put(TableMetadata.TableType.Approval, "approvalRepeatableQGObservations.sql");
        this.put(TableMetadata.TableType.Rejection, "rejectionRepeatableQGObservations.sql");
    }};

    private static String toString(String uuid) {
        return uuid == null ? "" : uuid;
    }

    public boolean supports(TableMetadata tableMetadata) {
        return tableMetadata.getType().equals(TableMetadata.Type.RepeatableQuestionGroup);
    }

    public String generateSql(TableMetadata tableMetadata, Date startTime, Date endTime) {
        if (supports(tableMetadata)) {
            String fileName = "repeatableQG/" + parentTypeFileMap.get(tableMetadata.getParentTableType());
            return getSql(fileName, tableMetadata, startTime, endTime);
        }
        throw new RuntimeException("Could not generate sql for" + tableMetadata.getType().toString());
    }

    public String getSql(String fileName, TableMetadata tableMetadata, Date startTime, Date endTime) {
        String template = readSqlFile(fileName);
        return template.replace("${schema_name}", OrgIdentityContextHolder.getDbSchema())
                .replace("${table_name}", tableMetadata.getName())
                .replace("${observations_to_insert_list}", TransactionDataSyncHelper.getListOfObservations(tableMetadata))
                .replace("${concept_maps}", TransactionDataSyncHelper.getConceptMaps(tableMetadata))
                .replace("${cross_join_concept_maps}", "cross join " + TransactionDataSyncHelper.getConceptMapName(tableMetadata))
                .replace("${subject_type_uuid}", toString(tableMetadata.getSubjectTypeUuid()))
                .replace("${selections}", TransactionDataSyncHelper.buildObservationSelection(tableMetadata, "observations"))
                .replace("${encounter_type_uuid}", toString(tableMetadata.getEncounterTypeUuid()))
                .replace("${program_uuid}", toString(tableMetadata.getProgramUuid()))
                .replace("${repeatable_question_group_concept_uuid}", toString(tableMetadata.getRepeatableQuestionGroupConceptUuid()))
                // Empty for the four non-decision parents, whose templates carry no such placeholder.
                .replace("${approval_entity_type}", TransactionDataSyncHelper.approvalEntityType(tableMetadata))
                .replace("${approval_entity_type_uuid}", TransactionDataSyncHelper.approvalEntityTypeUuid(tableMetadata))
                .replace("${approval_program_filter}", TransactionDataSyncHelper.approvalProgramFilter(tableMetadata))
                .replace("${start_time}", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(startTime))
                .replace("${end_time}", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(endTime));
    }
}
