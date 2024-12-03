package org.avniproject.etl.repository;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Repository
public class ColumnMetadataRepository {
    private final JdbcTemplate jdbcTemplate;

    public ColumnMetadataRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<ColumnMetadata> saveColumns(TableMetadata tableMetadata) {
        return tableMetadata
                .getColumnMetadataList()
                .stream()
                .map(columnMetadata -> saveColumn(tableMetadata.getId(), columnMetadata))
                .collect(Collectors.toList());
    }

    private ColumnMetadata saveColumn(Integer tableId, ColumnMetadata columnMetadata) {
        return columnMetadata.getId() == null ? insert(tableId, columnMetadata) : update(tableId, columnMetadata);
    }

    private ColumnMetadata update(Integer tableId, ColumnMetadata columnMetadata) {
        String sql = """
            update column_metadata
            set name = :name,
                type = :type,
                table_id = :table_id,
                concept_id = :concept_id,
                concept_voided = :concept_voided
            where id = :id;
        """;
        new NamedParameterJdbcTemplate(jdbcTemplate).update(sql, addParameters(tableId, columnMetadata));

        return columnMetadata;
    }

    private ColumnMetadata insert(Integer tableId, ColumnMetadata columnMetadata) {
        Number id = new SimpleJdbcInsert(jdbcTemplate)
                .withTableName("column_metadata")
                .usingGeneratedKeyColumns("id")
                .executeAndReturnKey(addParameters(tableId, columnMetadata));
        columnMetadata.setId(id.intValue());

        return columnMetadata;
    }

    private Map<String, Object> addParameters(Integer tableId, ColumnMetadata columnMetadata) {
        Map<String, Object> parameters = new HashMap<>(1);
        parameters.put("id", columnMetadata.getId());
        parameters.put("schema_name", OrgIdentityContextHolder.getDbSchema());
        parameters.put("table_id", tableId);
        parameters.put("name", columnMetadata.getName());
        parameters.put("type", columnMetadata.getType().toString());
        parameters.put("concept_id", columnMetadata.getConceptId());
        parameters.put("concept_type", columnMetadata.getConceptType() != null ? columnMetadata.getConceptType().toString() : null);
        parameters.put("concept_uuid", columnMetadata.getConceptUuid());
        parameters.put("parent_concept_uuid", columnMetadata.getParentConceptUuid());
        parameters.put("concept_voided", columnMetadata.isConceptVoided());
        return parameters;
    }
}
