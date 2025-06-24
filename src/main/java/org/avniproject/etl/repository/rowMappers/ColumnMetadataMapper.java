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

        boolean hasParentConcept = column.get("parent_concept_name") != null;
        if (hasParentConcept) {
            boolean isQuestionGroup = hasParentConcept && !TableMetadata.Type.RepeatableQuestionGroup.name().equals(column.get("table_type"));
            String conceptName = isQuestionGroup ? column.get("parent_concept_name") + " " + column.get("concept_name") : (String) column.get("concept_name");
            return new ColumnMetadata(
                    null,
                    conceptName,
                    (Integer) column.get("concept_id"),
                    ColumnMetadata.ConceptType.valueOf((String) column.get("element_type")),
                    (String) column.get("concept_uuid"),
                    column.get("parent_concept_uuid") == null ? null : (String) column.get("parent_concept_uuid"),
                    column.get("parent_concept_name") == null ? null : (String) column.get("parent_concept_name"),
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
                null,
                columnType, false);
    }

    @Override
    public ColumnMetadata mapRow(ResultSet rs, int rowNum) throws SQLException {
        String parentConceptUuid = null, parentConceptName = null;

        try {
            parentConceptUuid = rs.getString("parent_concept_uuid");
            parentConceptName = rs.getString("parent_concept_name");
        } catch (SQLException e) {
            // ignore
        }
        
        return new ColumnMetadata(
                rs.getInt("id"),
                new Column(rs.getString("name"), Column.Type.valueOf(rs.getString("type"))),
                rs.getInt("concept_id"),
                ColumnMetadata.ConceptType.valueOf(rs.getString("concept_type")),
                rs.getString("concept_uuid"),
                parentConceptUuid,
                parentConceptName,
                rs.getBoolean("concept_voided"));
    }
}
