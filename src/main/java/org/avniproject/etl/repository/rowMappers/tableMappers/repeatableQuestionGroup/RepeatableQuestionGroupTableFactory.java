package org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup;

import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.Table;

import java.util.Map;

public class RepeatableQuestionGroupTableFactory {
    /**
     * Null when the parent form type is not one a repeatable question group can hang off (#174, AC11).
     * repeatableQuestionGroups.sql takes parent_table_type straight from f.form_type with no filter, so an
     * Approval or Rejection form containing a repeatable question group reaches here - and TableType.valueOf
     * would abort the whole organisation's run. Question groups inside approval forms are out of scope for
     * #174, so no secondary table is the right outcome; a dead ETL is not.
     */
    public static Table create(Map<String, Object> tableDetails) {
        String parentTableType = (String) tableDetails.get("parent_table_type");
        TableMetadata.TableType parentType;
        try {
            parentType = TableMetadata.TableType.valueOf(parentTableType);
        } catch (IllegalArgumentException e) {
            return null;
        }
        return switch (parentType) {
            case IndividualProfile -> new SubjectRepeatableQuestionGroupTable();
            case ProgramEnrolment -> new ProgramEnrolmentRepeatableQuestionGroupTable();
            case ProgramEncounter -> new ProgramEncounterRepeatableQuestionGroupTable();
            case Encounter -> new EncounterRepeatableQuestionGroupTable();
        };
    }
}
