package org.avniproject.etl.repository;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.dto.TableMetadataST;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Repository
public class TableMetadataRepository {
    private final JdbcTemplate jdbcTemplate;

    private final ColumnMetadataRepository columnMetadataRepository;
    private final IndexMetadataRepository indexMetadataRepository;

    @Autowired
    public TableMetadataRepository(JdbcTemplate jdbcTemplate, ColumnMetadataRepository columnMetadataRepository, IndexMetadataRepository indexMetadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.columnMetadataRepository = columnMetadataRepository;
        this.indexMetadataRepository = indexMetadataRepository;
    }

    public TableMetadata save(TableMetadata tableMetadata) {
        TableMetadata savedTableMetadata = saveTable(tableMetadata);
        savedTableMetadata.setColumnMetadataList(columnMetadataRepository.saveColumns(savedTableMetadata));
        savedTableMetadata.setIndexMetadataList(indexMetadataRepository.save(tableMetadata));

        return savedTableMetadata;
    }

    private TableMetadata saveTable(TableMetadata tableMetadata) {
        return tableMetadata.getId() == null ?
                insert(tableMetadata) :
                update(tableMetadata);
    }

    public TableMetadata update(TableMetadata tableMetadata) {
        String sql = "update table_metadata\n" +
                "set name              = :name,\n" +
                "    type              = :type,\n" +
                "    subject_type_uuid   = :subject_type_uuid,\n" +
                "    program_uuid        = :program_uuid,\n" +
                "    encounter_type_uuid = :encounter_type_uuid,\n" +
                "    form_uuid           = :form_uuid,\n" +
                "    repeatable_question_group_concept_uuid = :repeatable_question_group_concept_uuid\n" +
                "where id = :id;";

        new NamedParameterJdbcTemplate(jdbcTemplate).update(sql, addParameters(tableMetadata));
        return tableMetadata;
    }

    public TableMetadata insert(TableMetadata tableMetadata) {
        Number id = new SimpleJdbcInsert(jdbcTemplate).withTableName("table_metadata")
                .usingGeneratedKeyColumns("id")
                .executeAndReturnKey(addParameters(tableMetadata));

        tableMetadata.setId(id.intValue());
        return tableMetadata;
    }

    private Map<String, Object> addParameters(TableMetadata tableMetadata) {
        Map<String, Object> parameters = new HashMap<>(1);
        parameters.put("id", tableMetadata.getId());
        parameters.put("schema_name", OrgIdentityContextHolder.getDbSchema());
        parameters.put("name", tableMetadata.getName());
        parameters.put("type", tableMetadata.getType().toString());
        parameters.put("subject_type_uuid", tableMetadata.getSubjectTypeUuid());
        parameters.put("group_subject_type_uuid", tableMetadata.getGroupSubjectTypeUuid());
        parameters.put("member_subject_type_uuid", tableMetadata.getMemberSubjectTypeUuid());
        parameters.put("program_uuid", tableMetadata.getProgramUuid());
        parameters.put("encounter_type_uuid", tableMetadata.getEncounterTypeUuid());
        parameters.put("form_uuid", tableMetadata.getFormUuid());
        parameters.put("repeatable_question_group_concept_uuid", tableMetadata.getRepeatableQuestionGroupConceptUuid());

        return parameters;
    }

    public List<TableMetadataST> fetchByType(List<TableMetadata.Type> types) {
        List<String> list = types.stream().map(Enum::name).toList();
        String sql = "SELECT * FROM table_metadata WHERE type IN (:types)";

        HashMap<String, Object> params = new HashMap<>();
        params.put("types", list);
        return runInOrgContext(() -> new NamedParameterJdbcTemplate(jdbcTemplate)
                .query(sql, params, (rs, rowNum) ->
                        new TableMetadataST(rs.getString("name"),
                                rs.getString("type"),
                                rs.getString("subject_type_uuid"),
                                rs.getString("program_uuid"),
                                rs.getString("encounter_type_uuid"),
                                rs.getString("type").equals("Person")
                        )), jdbcTemplate);
    }

}
