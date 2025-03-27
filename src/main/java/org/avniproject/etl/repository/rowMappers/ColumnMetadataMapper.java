package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class ColumnMetadataMapper implements RowMapper<ColumnMetadata> {
    public ColumnMetadata create(Map<String, Object> column) {
        // Column metadata is used by Address properties which are not supported right now for voiding, renaming
        Object isVoided = column.get("concept_voided");
        if (isVoided == null) {
            isVoided = false;
        }

        boolean isQuestionGroup = column.get("parent_concept_name") != null && !TableMetadata.Type.RepeatableQuestionGroup.name().equals(column.get("table_type"));
        if (isQuestionGroup) {
            return new ColumnMetadata(
                    null,
                    column.get("parent_concept_name") + " " + column.get("concept_name"),
                    (Integer) column.get("concept_id"),
                    ColumnMetadata.ConceptType.valueOf((String) column.get("element_type")),
                    (String) column.get("concept_uuid"),
                    (String) column.get("parent_concept_uuid"),
                    null,
                    (Boolean) isVoided);
        }
        return new ColumnMetadata(
                null,
                (String) column.get("concept_name"),
                (Integer) column.get("concept_id"),
                ColumnMetadata.ConceptType.valueOf((String) column.get("element_type")),
                (String) column.get("concept_uuid"),
                null,
                null,
                (Boolean) isVoided);
    }

    public ColumnMetadata createSyncColumnMetadata(Map<String, Object> column, Column.ColumnType columnType) {
        return new ColumnMetadata(
                null,
                (String) column.get("concept_name"),
                (Integer) column.get("concept_id"),
                ColumnMetadata.ConceptType.valueOf((String) column.get("element_type")),
                (String) column.get("concept_uuid"),
                null,
                columnType, false);
    }

    @Override
    public ColumnMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new ColumnMetadata(
                rs.getInt("id"),
                new Column(rs.getString("name"), Column.Type.valueOf(rs.getString("type"))),
                rs.getInt("concept_id"),
                ColumnMetadata.ConceptType.valueOf(rs.getString("concept_type")),
                rs.getString("concept_uuid"),
                rs.getString("parent_concept_uuid"),
                rs.getBoolean("concept_voided"));
    }
}
