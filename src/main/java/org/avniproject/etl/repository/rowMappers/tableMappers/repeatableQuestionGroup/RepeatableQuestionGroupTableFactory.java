package org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup;

import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.Table;

import java.util.Map;

public class RepeatableQuestionGroupTableFactory {
    public static Table create(Map<String, Object> tableDetails) {
        String parentTableType = (String) tableDetails.get("parent_table_type");
        return switch (TableMetadata.TableType.valueOf(parentTableType)) {
            case IndividualProfile -> new SubjectRepeatableQuestionGroupTable();
            case ProgramEnrolment -> new ProgramEnrolmentRepeatableQuestionGroupTable();
            case ProgramEncounter -> new ProgramEncounterRepeatableQuestionGroupTable();
            case Encounter -> new EncounterRepeatableQuestionGroupTable();
        };
    }
}
