package org.avniproject.etl.repository;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.metadata.diff.Diff;
import org.avniproject.etl.repository.rowMappers.*;
import org.avniproject.etl.repository.rowMappers.tableMappers.AddressTable;
import org.avniproject.etl.repository.rowMappers.tableMappers.ChecklistTable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;
import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

@Repository
public class SchemaMetadataRepository {
    private static final String PLACEHOLDER_CONCEPT_UUID = "b4e5a662-97bf-4846-b9b7-9baeab4d89c4";
    private final JdbcTemplate jdbcTemplate;
    private final TableMetadataRepository tableMetadataRepository;

    @Autowired
    public SchemaMetadataRepository(JdbcTemplate jdbcTemplate, TableMetadataRepository tableMetadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableMetadataRepository = tableMetadataRepository;
    }

    @Transactional(readOnly = true)
    public SchemaMetadata getNewSchemaMetadata() {
        List<TableMetadata> tables = new ArrayList<>(getFormTables());
        tables.add(getAddressTable());
        tables.add(MediaTableMetadataBuilder.build());
        tables.add(MediaAnalysisTableMetadataBuilder.build());
        tables.add(SyncTelemetryTableMetadataBuilder.build());
        tables.add(UserTableMetadataBuilder.build());
        tables.addAll(getGroupSubjectTables());

        tables.addAll(getRepeatableQuestionGroupTables());
        //tables.addAll(getChecklistTables());
        return new SchemaMetadata(tables);
    }

    private TableMetadata getAddressTable() {
        String locationPropertiesSql = "select (case when c.data_type = 'Coded' then fe.type else c.data_type end) as element_type,\n" +
                "       c.id                                                                   concept_id,\n" +
                "       c.uuid                                                                 concept_uuid,\n" +
                "       c.name                                                                 concept_name\n" +
                "from form f\n" +
                "         join form_element_group feg on f.id = feg.form_id\n" +
                "         join form_element fe on feg.id = fe.form_element_group_id and fe.is_voided = false\n" +
                "         join concept c on fe.concept_id = c.id\n" +
                "where f.form_type = 'Location'\n" +
                "  and f.is_voided = false";
        List<Map<String, Object>> addressLevelTypes = runInOrgContext(() -> jdbcTemplate.queryForList("select name from address_level_type where not is_voided;"), jdbcTemplate);
        List<Map<String, Object>> locationProperties = runInOrgContext(() -> jdbcTemplate.queryForList(locationPropertiesSql), jdbcTemplate);
        AddressTable addressTable = new AddressTable(addressLevelTypes, locationProperties);
        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setName(addressTable.name(null));
        tableMetadata.setType(TableMetadata.Type.Address);
        tableMetadata.setColumnMetadataList(addressTable.columnMetadata());
        return tableMetadata;
    }

    private List<TableMetadata> getRepeatableQuestionGroupTables() {
        String sqlFormat = readSqlFile("repeatableQG/repeatableQuestionGroups.sql");
        String sql = format(sqlFormat, PLACEHOLDER_CONCEPT_UUID);
        return getTableMetadataForForm(sql);
    }

    private List<TableMetadata> getFormTables() {
        String sql = format("select fm.uuid                                                           form_mapping_uuid,\n" +
                "       f.uuid                                                                 form_uuid,\n" +
                "       ost.name                                                               subject_type_name,\n" +
                "       st.uuid                                                                subject_type_uuid,\n" +
                "       st.type                                                                subject_type_type,\n" +
                "       st.allow_middle_name                                                   subject_type_allow_middle_name,\n" +
                "       f.form_type                                                            table_type,\n" +
                "       p.uuid                                                                 program_uuid,\n" +
                "       op.name                                                                program_name,\n" +
                "       et.uuid                                                                encounter_type_uuid,\n" +
                "       oet.name                                                               encounter_type_name,\n" +
                "       fm.enable_approval                                                     enable_approval,\n" +
                "       c.name                                                                 concept_name,\n" +
                "       gc.name                                                                parent_concept_name,\n" +
                "       c.id                                                                   concept_id,\n" +
                "       gc.id                                                                  parent_concept_id,\n" +
                "       c.uuid                                                                 concept_uuid,\n" +
                "       gc.uuid                                                                parent_concept_uuid,\n" +
                "       (case when c.data_type = 'Coded' then fe.type else c.data_type end) as element_type,\n" +
                "       (case when gc.data_type = 'Coded' then gfe.type else gc.data_type end) as parent_element_type\n" +
                "from form_mapping fm\n" +
                "         inner join form f on fm.form_id = f.id\n" +
                "         left outer join form_element_group feg on f.id = feg.form_id\n" +
                "         left outer join form_element fe on feg.id = fe.form_element_group_id\n and fe.is_voided is false" +
                "         left outer join form_element gfe on gfe.id = fe.group_id\n and gfe.is_voided is false" +
                "         left outer join concept c on fe.concept_id = c.id and c.data_type <> 'QuestionGroup' and c.uuid <> '%s'\n" +
                "         left outer join concept gc on gfe.concept_id = gc.id\n" +
                "         inner join subject_type st on fm.subject_type_id = st.id\n" +
                "         inner join operational_subject_type ost on st.id = ost.subject_type_id\n" +
                "         left outer join program p on fm.entity_id = p.id\n" +
                "         left outer join operational_program op on p.id = op.program_id\n" +
                "         left outer join encounter_type et on fm.observations_type_entity_id = et.id\n" +
                "         left outer join operational_encounter_type oet on et.id = oet.encounter_type_id\n" +
                "         left outer join non_applicable_form_element nafe on fe.id = nafe.form_element_id\n\n" +
                "where fm.is_voided is false" +
                " and (p.id is null or op.id is not null) and (et.id is null or oet.id is not null)" +
                " and nafe.id is null;", PLACEHOLDER_CONCEPT_UUID);

        List<TableMetadata> tables = getTableMetadataForForm(sql);
        tables.forEach(this::addDecisionConceptColumns);
        tables.stream().filter(t -> !t.isSubjectTable()).forEach(this::addSyncAttributeColumns);
        return tables;
    }

    private List<TableMetadata> getChecklistTables() {
        String sql = readSqlFile("checklist/checklistMetadata.sql");
        List<Map<String, Object>> checklistTypes = runInOrgContext(() -> jdbcTemplate.queryForList(sql), jdbcTemplate);
        ChecklistTable checklistTable = new ChecklistTable(checklistTypes);

        Map<String, Object> tableDetails = checklistTypes.get(0);

        return checklistTypes.stream().map(x -> {
            TableMetadata tableMetadata = new TableMetadata();
            tableMetadata.setName(checklistTable.name(tableDetails));
            tableMetadata.setType(TableMetadata.Type.Checklist);
            return tableMetadata;
        }).collect(Collectors.toList());
    }

    private List<TableMetadata> getTableMetadataForForm(String sql) {
        List<Map<String, Object>> maps = runInOrgContext(() -> jdbcTemplate.queryForList(sql), jdbcTemplate);
        Map<Object, List<Map<String, Object>>> tableMaps = maps.stream().collect(Collectors.groupingBy(stringObjectMap -> {
            String formMappingUUID = stringObjectMap.get("form_mapping_uuid").toString();
            Object rqgConceptUUID = stringObjectMap.get("repeatable_question_group_concept_uuid");
            return rqgConceptUUID == null ? formMappingUUID : formMappingUUID + rqgConceptUUID;
        }));
        return tableMaps.values().stream().map(mapList -> new TableMetadataMapper().create(mapList)).collect(Collectors.toList());
    }

    private void addDecisionConceptColumns(TableMetadata tableMetadata) {
        String sql = "select c.id                                                                      as concept_id,\n" +
                "       c.uuid                                                                    as concept_uuid,\n" +
                "       c.name                                                                    as concept_name,\n" +
                "       (case when c.data_type = 'Coded' then 'MultiSelect' else c.data_type end) as element_type\n" +
                "from decision_concept dc\n" +
                "         inner join concept c on dc.concept_id = c.id\n" +
                "         inner join form f on f.id = dc.form_id\n" +
                "where f.uuid = ?;";

        List<Map<String, Object>> conceptIds = runInOrgContext(() -> jdbcTemplate.queryForList(sql, tableMetadata.getFormUuid()), jdbcTemplate);

        tableMetadata.addColumnMetadata(
                new ArrayList<>(conceptIds
                        .stream()
                        .map(column -> new ColumnMetadataMapper().create(column))
                        .collect(Collectors.toMap(
                                ColumnMetadata::getName,
                                obj -> obj,
                                (first, second) -> first
                        )).values()));
    }

    private void addSyncAttributeColumns(TableMetadata tableMetadata) {
        List<Map<String, Object>> syncRegistrationConcept1 = runInOrgContext(() -> jdbcTemplate.queryForList(getSyncAttributeSql("sync_registration_concept_1")), jdbcTemplate);
        List<Map<String, Object>> syncRegistrationConcept2 = runInOrgContext(() -> jdbcTemplate.queryForList(getSyncAttributeSql("sync_registration_concept_2")), jdbcTemplate);
        addSyncAttribute(tableMetadata, syncRegistrationConcept1, Column.ColumnType.syncAttribute1);
        addSyncAttribute(tableMetadata, syncRegistrationConcept2, Column.ColumnType.syncAttribute2);
    }

    private void addSyncAttribute(TableMetadata tableMetadata, List<Map<String, Object>> syncRegistrationConcept, Column.ColumnType columnType) {
        tableMetadata.addColumnMetadata(
                new ArrayList<>(syncRegistrationConcept
                        .stream()
                        .map(column -> new ColumnMetadataMapper().createSyncColumnMetadata(column, columnType))
                        .collect(Collectors.toMap(
                                ColumnMetadata::getName,
                                obj -> obj,
                                (first, second) -> first
                        )).values()));
    }

    private List<TableMetadata> getGroupSubjectTables() {
        String sql = format("select\n" +
                "    st.uuid                                                                group_subject_type_uuid,\n" +
                "    ost.name                                                               group_subject_type_name,\n" +
                "    gr.role                                                                                as role,\n" +
                "    mst.uuid                                                              member_subject_type_uuid,\n" +
                "    most.name                                                             member_subject_type_name,\n" +
                "    CASE\n" +
                "        WHEN (st.type = 'Group') THEN 'GroupToMember'\n" +
                "        ELSE 'HouseholdToMember'\n" +
                "    END AS table_type\n" +
                "from form_mapping fm\n" +
                "         inner join form f on fm.form_id = f.id\n" +
                "         inner join subject_type st on fm.subject_type_id = st.id\n" +
                "         inner join operational_subject_type ost on st.id = ost.subject_type_id\n" +
                "         inner join group_role gr on gr.group_subject_type_id = st.id\n" +
                "         inner join subject_type mst on gr.member_subject_type_id = mst.id\n" +
                "         inner join operational_subject_type most on mst.id = most.subject_type_id\n" +
                "where fm.is_voided is false and f.form_type = 'IndividualProfile' and st.type in ('Group','Household');");

        List<Map<String, Object>> maps = runInOrgContext(() -> jdbcTemplate.queryForList(sql), jdbcTemplate);
        Map<Object, List<Map<String, Object>>> tableMaps = maps.stream().collect(Collectors.groupingBy(stringObjectMap ->
                stringObjectMap.get("group_subject_type_uuid").toString() + stringObjectMap.get("member_subject_type_uuid").toString()));
        List<TableMetadata> tables = tableMaps.values().stream().map(mapList -> new TableMetadataMapper().create(mapList)).collect(Collectors.toList());
        tables.stream().filter(t -> t.getType().equals(TableMetadata.Type.HouseholdToMember)).forEach(this::addHeadOfHouseholdColumns);
        return tables;
    }

    private void addHeadOfHouseholdColumns(TableMetadata tableMetadata) {
        ColumnMetadata headOfHouseholdIDColumnMetadata = new ColumnMetadata(new Column( "Head of household ID",
                Column.Type.integer, Column.ColumnType.index), null, null, null);
        tableMetadata.addColumnMetadata(Arrays.asList(headOfHouseholdIDColumnMetadata));
    }

    private String getSyncAttributeSql(String columnName) {
        return format("select c.id                                                             concept_id,\n" +
                "       c.name                                                                 concept_name,\n" +
                "       c.uuid                                                                 concept_uuid,\n" +
                "       (case when c.data_type = 'Coded' then fe.type else c.data_type end) as element_type\n" +
                "from form_mapping fm\n" +
                "         join subject_type st on fm.subject_type_id = st.id\n" +
                "         join form f on f.id = fm.form_id\n" +
                "         join form_element_group feg on feg.form_id = f.id and feg.is_voided = false\n" +
                "         join form_element fe on fe.form_element_group_id = feg.id and fe.is_voided = false\n" +
                "         join concept c on c.uuid = st.%s and c.id = fe.concept_id\n" +
                "where c.id notnull\n" +
                "  and fm.is_voided = false\n" +
                "  and f.form_type = 'IndividualProfile'", columnName);
    }

    @Transactional(readOnly = true)
    public SchemaMetadata getExistingSchemaMetadata() {
        String getTablesAndColumnsSql = format(
                "select tm.id                table_id,\n" +
                        "       tm.type                                 table_type,\n" +
                        "       tm.name                                 table_name,\n" +
                        "       tm.form_uuid                            form_uuid,\n" +
                        "       tm.encounter_type_uuid                  encounter_type_uuid,\n" +
                        "       tm.program_uuid                         program_uuid,\n" +
                        "       tm.subject_type_uuid                    subject_type_uuid,\n" +
                        "       tm.group_subject_type_uuid              group_subject_type_uuid,\n" +
                        "       tm.member_subject_type_uuid             member_subject_type_uuid,\n" +
                        "       cm.id                                   column_id,\n" +
                        "       cm.type                                 column_type,\n" +
                        "       cm.concept_type                         concept_type,\n" +
                        "       cm.concept_id                           concept_id,\n" +
                        "       cm.name                                 concept_name,\n" +
                        "       cm.concept_uuid                         concept_uuid,\n" +
                        "       cm.parent_concept_uuid                  parent_concept_uuid,\n" +
                        "       tm.repeatable_question_group_concept_uuid                  repeatable_question_group_concept_uuid\n" +
                        "from table_metadata tm\n" +
                        "         left outer join column_metadata cm on tm.id = cm.table_id\n" +
                        "     where tm.schema_name = '%s';", OrgIdentityContextHolder.getDbSchema());

        String getIndicesSql = format(
                "select tm.id                  table_id,\n" +
                        "       tm.type                table_type,\n" +
                        "       tm.name                table_name,\n" +
                        "       im.id                  index_id,\n" +
                        "       im.name                index_name,\n" +
                        "       tm.form_uuid           form_uuid,\n" +
                        "       tm.encounter_type_uuid encounter_type_uuid,\n" +
                        "       tm.program_uuid        program_uuid,\n" +
                        "       tm.subject_type_uuid   subject_type_uuid,\n" +
                        "       cm.id                  column_id,\n" +
                        "       cm.type                column_type,\n" +
                        "       cm.concept_type        concept_type,\n" +
                        "       cm.concept_id          concept_id,\n" +
                        "       cm.name                concept_name,\n" +
                        "       cm.concept_uuid        concept_uuid,\n" +
                        "       cm.parent_concept_uuid parent_concept_uuid,\n" +
                        "       tm.repeatable_question_group_concept_uuid                  repeatable_question_group_concept_uuid\n" +
                        "from table_metadata tm\n" +
                        "         inner join index_metadata im on tm.id = im.table_metadata_id\n" +
                        "         inner join column_metadata cm on im.column_id = cm.id\n" +
                        "where tm.schema_name = '%s';", OrgIdentityContextHolder.getDbSchema());

        List<Map<String, Object>> tableAndColumnMaps = jdbcTemplate.queryForList(getTablesAndColumnsSql);
        List<Map<String, Object>> indexMaps = jdbcTemplate.queryForList(getIndicesSql);
        Map<Object, List<Map<String, Object>>> tableMaps = tableAndColumnMaps.stream().collect(Collectors.groupingBy(stringObjectMap -> stringObjectMap.get("table_id")));
        Map<Object, List<Map<String, Object>>> indices = indexMaps.stream().collect(Collectors.groupingBy(stringObjectMap -> stringObjectMap.get("table_id")));
        List<TableMetadata> tables = tableMaps.entrySet().stream().map(table -> new TableMetadataMapper().createFromExistingSchemaMetaData(table.getValue(), indices.getOrDefault(table.getKey(), new ArrayList<>()))).collect(Collectors.toList());
        return new SchemaMetadata(tables);
    }

    public void applyChanges(List<Diff> changes) {
        changes.forEach(change -> jdbcTemplate.execute(change.getSql()));
    }

    public void save(SchemaMetadata schemaMetadata) {
        schemaMetadata.setTableMetadata(
                schemaMetadata.getOrderedTableMetadata()
                        .stream()
                        .map(tableMetadataRepository::save)
                        .collect(Collectors.toList()));
    }
}
