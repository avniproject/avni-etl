package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;

import java.util.Map;

public class ColumnMetadataMapper {
    public ColumnMetadata create(Map<String, Object> column) {
        // Column metadata is used by Address properties which are not supported right now for voiding, renaming
        Object isVoided = column.get("concept_voided");
        if (isVoided == null) {
            isVoided = false;
        }

        if (column.get("parent_concept_name") != null) {
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
}
