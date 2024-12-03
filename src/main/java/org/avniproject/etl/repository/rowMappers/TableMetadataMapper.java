package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.IndexMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.*;
import org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup.RepeatableQuestionGroupTableFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableMetadataMapper {
    private ColumnMetadata createColumnMetaData(Map<String, Object> map) {
        return new ColumnMetadata(
                (Integer) map.get("column_id"),
                new Column(
                        (String) map.get("concept_name"),
                        Column.Type.valueOf((String) map.get("column_type"))
                ),
                (Integer) map.get("concept_id"),
                map.get("concept_type") == null ? null : ColumnMetadata.ConceptType.valueOf((String) map.get("concept_type")),
                (String) map.get("concept_uuid"),
                (String) map.get("parent_concept_uuid"),
                (Boolean) map.get("concept_voided"));
    }

    public TableMetadata createFromExistingSchemaMetaData(List<Map<String, Object>> columns, List<Map<String, Object>> indices) {
        TableMetadata tableMetadata = new TableMetadata();
        Map<String, Object> tableDetails = columns.get(0);
        populateCommonColumns(tableMetadata, tableDetails);

        tableMetadata.setName((String) tableDetails.get("table_name"));
        tableMetadata.setId((Integer) tableDetails.get("table_id"));

        tableMetadata.addColumnMetadata(columns.stream()
                .filter(stringObjectMap -> stringObjectMap.get("column_id") != null)
                .map(this::createColumnMetaData)
                .collect(Collectors.toList()));

        tableMetadata.addIndexMetadata(indices.stream().map(index -> {
                    ColumnMetadata columnMetadata = tableMetadata.getColumn((Integer) index.get("column_id"));
                    return new IndexMetadata((Integer) index.get("index_id"), (String) index.get("index_name"), columnMetadata);
                })
                .collect(Collectors.toList()));

        return tableMetadata;
    }

    public TableMetadata create(List<Map<String, Object>> columns) {
        TableMetadata tableMetadata = new TableMetadata();
        Map<String, Object> tableDetails = columns.get(0);
        populateCommonColumns(tableMetadata, tableDetails);
        Table table = getTableStructure(tableMetadata.getType(), tableDetails);
        tableMetadata.setName(table.name(tableDetails));

        tableMetadata.addColumnMetadata(table.columns().stream().map(column -> new ColumnMetadata(column, null, null, null, false)).collect(Collectors.toList()));
        tableMetadata.addColumnMetadata(columns.stream()
                .filter(stringObjectMap -> stringObjectMap.get("concept_id") != null)
                .map(column -> new ColumnMetadataMapper().create(column)).collect(Collectors.toList()));

        table.columns().forEach(column -> {
            if (column.isIndexed()) {
                tableMetadata.addIndexMetadata(column);
            }
        });

        return tableMetadata;
    }

    private void populateCommonColumns(TableMetadata tableMetadata, Map<String, Object> tableDetails) {
        tableMetadata.setFormUuid(((String) tableDetails.get("form_uuid")));
        tableMetadata.setType(getTableType(tableDetails));
        tableMetadata.setSubjectTypeUuid((String) tableDetails.get("subject_type_uuid"));
        tableMetadata.setGroupSubjectTypeUuid((String) tableDetails.get("group_subject_type_uuid"));
        tableMetadata.setMemberSubjectTypeUuid((String) tableDetails.get("member_subject_type_uuid"));
        tableMetadata.setEncounterTypeUuid((String) tableDetails.get("encounter_type_uuid"));
        tableMetadata.setProgramUuid((String) tableDetails.get("program_uuid"));
        if (tableDetails.get("table_type").equals("RepeatableQuestionGroup")) {
            tableMetadata.setRepeatableQuestionGroupConceptUuid((String) tableDetails.get("repeatable_question_group_concept_uuid"));
        }
    }

    private TableMetadata.Type getTableType(Map<String, Object> tableDetails) {
        String tableType = (String) tableDetails.get("table_type");
        return tableType.equals(TableMetadata.TableType.IndividualProfile.name()) ?
                TableMetadata.Type.valueOf((String) tableDetails.get("subject_type_type")) :
                TableMetadata.Type.valueOf(tableType);
    }

    public Table getTableStructure(TableMetadata.Type type, Map<String, Object> tableDetails) {
        switch (type) {
            case Group:
            case Household:
            case Individual:
                return new SubjectTable();
            case GroupToMember:
            case HouseholdToMember:
                return new GroupToMemberTable();
            case Person:
                return new PersonTable((Boolean) tableDetails.get("subject_type_allow_middle_name"));
            case ProgramEnrolment:
                return new ProgramEnrolmentTable();
            case ProgramExit:
                return new ProgramExitTable();
            case ProgramEncounter:
                return new ProgramEncounterTable();
            case ProgramEncounterCancellation:
                return new ProgramEncounterCancellationTable();
            case Encounter:
                return new EncounterTable();
            case IndividualEncounterCancellation:
                return new EncounterCancellationTable();
            case ManualProgramEnrolmentEligibility:
                return new SubjectProgramEligibilityTable();
            case RepeatableQuestionGroup:
                return RepeatableQuestionGroupTableFactory.create(tableDetails);
            default:
                throw new RuntimeException("Cannot create table structure for table details: " + tableDetails);
        }
    }
}
