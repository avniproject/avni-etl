package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.IndexMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.rowMappers.tableMappers.*;
import org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup.RepeatableQuestionGroupTableFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TableMetadataMapper {
    private static final Logger logger = LoggerFactory.getLogger(TableMetadataMapper.class);

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
                (String) map.get("parent_concept_name"),
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

    /**
     * Null when this build cannot map the row - an unrecognised form type, or a repeatable question group
     * hanging off one. Callers filter nulls out; see AC11 on #174.
     */
    public TableMetadata create(List<Map<String, Object>> columns) {
        TableMetadata tableMetadata = new TableMetadata();
        Map<String, Object> tableDetails = columns.get(0);
        populateCommonColumns(tableMetadata, tableDetails);
        if (tableMetadata.getType() == null) return null;
        Table table = getTableStructure(tableMetadata.getType(), tableDetails);
        if (table == null) return null;
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

    /**
     * Null rather than an exception for a form type this build does not know (#174, AC11). getFormTables()
     * has no form-type filter, so a form type from a newer avni-server reaches here - and Type.valueOf
     * would take the whole organisation's ETL run down rather than one table.
     */
    private TableMetadata.Type getTableType(Map<String, Object> tableDetails) {
        String tableType = (String) tableDetails.get("table_type");
        String typeName = tableType.equals(TableMetadata.TableType.IndividualProfile.name()) ?
                (String) tableDetails.get("subject_type_type") : tableType;
        try {
            return TableMetadata.Type.valueOf(typeName);
        } catch (IllegalArgumentException e) {
            logger.warn("Skipping table for unrecognised form type '{}'. This build does not know it; reporting for the rest of the organisation is unaffected.", typeName);
            return null;
        }
    }

    public Table getTableStructure(TableMetadata.Type type, Map<String, Object> tableDetails) {
        if (type == null) return null;
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
            case Approval:
                return new ApprovalTable();
            case Rejection:
                return new RejectionTable();
            case RepeatableQuestionGroup:
                return RepeatableQuestionGroupTableFactory.create(tableDetails);
            default:
                // Skipped, not fatal - see getTableType.
                logger.warn("Skipping table: no table structure for type {}", type);
                return null;
        }
    }
}
