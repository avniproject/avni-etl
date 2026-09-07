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
        // #174. Same three parts as ProgramEncounter, so the same widths. A mapping with fewer parts
        // reads only the leading entries, so the shorter shapes are covered by the same list.
        put("Approval", List.of(6, 6, 20));
        put("Rejection", List.of(6, 6, 20));
    }};

    private String buildProperTableName(List<String> entities) {
        List<String> list = entities.stream()
                .map(String::toLowerCase)
                .map(e -> e.replaceAll("[^a-z0-9_\\s]", "").replaceAll("\\s+", "_"))
                .collect(Collectors.toList());
        return String.join("_", list);
    }

    public String generateName(List<String> entities, String tableType, String suffix) {
        // Absent parts are dropped rather than carried through. Approval and Rejection (#174) are the
        // first form types whose mapping shape varies - a subject-only mapping has no programme and no
        // encounter type - and buildProperTableName calls toLowerCase on every part. This is additive
        // for every existing caller: a null part throws today, so no working name can change.
        List<String> presentEntities = entities.stream().filter(Objects::nonNull).collect(Collectors.toList());
        List<String> entitiesWithSuffix = new ArrayList<>(presentEntities);
        if (suffix != null) {
            entitiesWithSuffix.add(suffix);
        }
        String tableName = buildProperTableName(entitiesWithSuffix);
        return tableName.length() > POSTGRES_MAX_TABLE_NAME_LENGTH ? getTrimmedTableName(presentEntities, tableType, suffix) : tableName;
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
