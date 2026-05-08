package org.avniproject.etl.repository.rowMappers;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TableNameGenerator {
    private static final int POSTGRES_MAX_TABLE_NAME_LENGTH = 63;

    public static final String RegistrationRepeatableQuestionGroup = "RegistrationRepeatableQuestionGroup";
    public static final String EncounterRepeatableQuestionGroup = "EncounterRepeatableQuestionGroup";
    public static final String ProgramEnrolmentRepeatableQuestionGroup = "ProgramEnrolmentRepeatableQuestionGroup";
    public static final String ProgramEncounterRepeatableQuestionGroup = "ProgramEncounterRepeatableQuestionGroup";

    private static final Map<String, List<Integer>> trims = new HashMap<>() {{
        put("Registration", List.of(6, 6));
        put(RegistrationRepeatableQuestionGroup, List.of(6, 20));
        put("Encounter", List.of(6, 20));
        put(EncounterRepeatableQuestionGroup, List.of(6, 20, 20));
        put("ProgramEnrolment", List.of(6, 20));
        put(ProgramEnrolmentRepeatableQuestionGroup, List.of(6, 20, 20));
        put("ProgramEncounter", List.of(6, 6, 20));
        put(ProgramEncounterRepeatableQuestionGroup, List.of(6, 6, 20, 20));
    }};

    private String buildProperTableName(List<String> entities) {
        List<String> list = entities.stream()
                .map(String::toLowerCase)
                .map(e -> e.replaceAll("[^a-z0-9_\\s]", "").replaceAll("\\s+", "_"))
                .collect(Collectors.toList());
        return String.join("_", list);
    }

    public String generateName(List<String> entities, String tableType, String suffix) {
        List<String> entitiesWithSuffix = new ArrayList<>(entities);
        if (suffix != null) {
            entitiesWithSuffix.add(suffix);
        }
        String tableName = buildProperTableName(entitiesWithSuffix);
        return tableName.length() > POSTGRES_MAX_TABLE_NAME_LENGTH ? getTrimmedTableName(entities, tableType, suffix) : tableName;
    }

    private String getTrimmedTableName(List<String> entities, String tableType, String suffix) {
        List<Integer> trimmingList = trims.get(tableType);
        List<String> trimmedNameList = IntStream
                .range(0, entities.size())
                .mapToObj(i -> getTrimmedName(entities, new StringBuilder(), trimmingList, i, suffix))
                .map(StringBuilder::toString)
                .collect(Collectors.toList());
        return buildProperTableName(trimmedNameList);
    }

    private StringBuilder getTrimmedName(List<String> entities, StringBuilder sb, List<Integer> trimmingList, int i, String suffix) {
        int lengthToConsider = trimmingList.get(i);
        String entityName = entities.get(i);
        if (lengthToConsider == 0) {
            sb.append(entityName);
        } else {
            String trimmedName = entityName.substring(0, Math.min(entityName.length(), lengthToConsider));
            sb.append(trimmedName);
        }
        if (suffix != null)
            sb.append(" ").append(suffix);
        return sb;
    }
}
