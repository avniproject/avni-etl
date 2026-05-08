package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.repository.rowMappers.TableNameGenerator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

abstract public class Table {
    // Probed in order; the first present value in tableDetails is used to disambiguate
    // names that would otherwise collide after truncation. Most-specific identifier first
    // so RQG tables pick the RQG concept UUID rather than the parent form mapping UUID.
    private static final List<String> DISAMBIGUATOR_KEYS = List.of(
            "repeatable_question_group_concept_uuid",
            "form_mapping_uuid",
            "subject_type_uuid",
            "group_subject_type_uuid"
    );

    abstract public String name(Map<String, Object> tableDetails);
    abstract public List<Column> columns();

    protected String generateTableName(String tableType, String suffix, Map<String, Object> tableDetails, String... partKeys) {
        TableNameGenerator tableNameGenerator = new TableNameGenerator();
        List<String> parts = Arrays.stream(partKeys).map(s -> (String) tableDetails.get(s)).collect(Collectors.toList());
        String disambiguatorUuid = pickDisambiguatorUuid(tableDetails);
        return tableNameGenerator.generateName(parts, tableType, suffix, disambiguatorUuid);
    }

    private static String pickDisambiguatorUuid(Map<String, Object> tableDetails) {
        if (tableDetails == null) return null;
        for (String key : DISAMBIGUATOR_KEYS) {
            Object value = tableDetails.get(key);
            if (value != null) return value.toString();
        }
        return null;
    }
}
