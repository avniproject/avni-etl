package org.avniproject.etl.repository.sql;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.avniproject.etl.domain.metadata.diff.Strings.COMMA;
import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

public class TransactionDataSyncHelper {
    private static final String obsSqlTemplate = readSqlFile("conceptMap.sql");

    public static String getConceptMapName(TableMetadata tableMetadata) {
        return tableMetadata.getName() + "_" + "concept_maps";
    }

    public static String getConceptMaps(TableMetadata tableMetadata) {
        List<String> names = new ArrayList<>();
        names.add("'dummy'");
        names.addAll(tableMetadata.getNonDefaultColumnMetadataList().stream().map(columnMetadata -> TransactionDataSyncHelper.wrapInSingleQuotes(columnMetadata.getConceptUuid())).collect(Collectors.toList()));

        return obsSqlTemplate
                .replace("${mapName}", getConceptMapName(tableMetadata))
                .replace("${conceptUuids}", String.join(", ", names));
    }

    public static StringBuffer getListOfObservations(TableMetadata tableMetadata) {
        StringBuffer list = new StringBuffer();
        if (tableMetadata.hasNonDefaultColumns()) {
            list.append(COMMA);
        }
        List<ColumnMetadata> columns = tableMetadata.getNonDefaultColumnMetadataList();
        if (columns.isEmpty()) return list;

        List<String> names = columns.stream().map(columnMetadata -> wrapInQuotes(columnMetadata.getName())).collect(Collectors.toList());
        String columnNames = String.join(", ", names);
        return list.append(columnNames);
    }

    public static String wrapInQuotes(String name) {
        return "\"" + name + "\"";
    }

    private static String orEmpty(String uuid) {
        return uuid == null ? "" : uuid;
    }

    /**
     * An approval or rejection form attaches to any of four mapping shapes, and the decision it produces
     * carries a different entity_type for each (#174). Shared by the decision tables and by the
     * repeatable question groups inside those forms, which need the identical filter - duplicating it is
     * what let the programme half go missing the first time.
     *
     * Mirrors avni-server's EntityApprovalStatusService.getEntityTypeUUID, which writes these columns.
     */
    public static String approvalEntityType(TableMetadata tableMetadata) {
        boolean hasProgram = tableMetadata.getProgramUuid() != null;
        boolean hasEncounterType = tableMetadata.getEncounterTypeUuid() != null;
        if (hasProgram && hasEncounterType) return "ProgramEncounter";
        if (hasEncounterType) return "Encounter";
        if (hasProgram) return "ProgramEnrolment";
        return "Subject";
    }

    /**
     * entity_type_uuid holds whichever type the decision is against: the encounter type for either
     * encounter shape, the programme for an enrolment, the subject type for a bare subject.
     */
    public static String approvalEntityTypeUuid(TableMetadata tableMetadata) {
        boolean hasProgram = tableMetadata.getProgramUuid() != null;
        boolean hasEncounterType = tableMetadata.getEncounterTypeUuid() != null;
        if (hasEncounterType) return orEmpty(tableMetadata.getEncounterTypeUuid());
        if (hasProgram) return orEmpty(tableMetadata.getProgramUuid());
        return orEmpty(tableMetadata.getSubjectTypeUuid());
    }

    /**
     * The programme half of a programme-visit mapping, which entity_type and entity_type_uuid cannot
     * carry (#174).
     *
     * A decision on a programme visit records entity_type ProgramEncounter and the visit type in
     * entity_type_uuid - the programme is nowhere on the row. Two mappings sharing a subject type and a
     * visit type across two programmes would otherwise generate a byte-identical filter, and each table
     * would collect the other's decisions. The programme is recovered by following entity_id to the
     * record judged. EXISTS rather than a join, because these templates promise one row per decision and
     * postgres plans a semi join, which cannot multiply rows.
     *
     * Empty for the other three shapes, which identify themselves.
     */
    public static String approvalProgramFilter(TableMetadata tableMetadata) {
        if (tableMetadata.getProgramUuid() == null || tableMetadata.getEncounterTypeUuid() == null) return "";
        return "AND EXISTS (SELECT 1 FROM public.program_encounter pe" +
                " JOIN public.program_enrolment pen ON pen.id = pe.program_enrolment_id" +
                " JOIN public.program prog ON prog.id = pen.program_id" +
                " WHERE pe.id = entity.entity_id AND prog.uuid = '" + tableMetadata.getProgramUuid() + "')";
    }

    private static String wrapInSingleQuotes(String name) {
        return "'" + name + "'";
    }

    public static String buildObservationSelection(TableMetadata tableMetadata, String obsColumnName) {
        List<ColumnMetadata> columns = tableMetadata.getNonDefaultColumnMetadataList();
        if (columns.isEmpty()) return "";

        String columnSelects = columns.parallelStream().map(column -> {
            String obsColumn = column.getColumn().isSyncAttributeColumn() ?
                    "ind.observations"
                    : String.format("entity.%s", obsColumnName);
            String columnName = column.getName();
            switch (column.getConceptType()) {
                case SingleSelect:
                case MultiSelect: {
                    return String.format("public.get_coded_string_value(%s%s, %s.map)::TEXT as \"%s\"",
                            obsColumn, column.getJsonbExtractor(), getConceptMapName(tableMetadata), column.getName());
                }
                case Date: {
                    return String.format("((%s%s)::timestamptz AT time zone 'asia/kolkata')::date as \"%s\"", obsColumn, column.getTextExtractor(), columnName);
                }
                case DateTime: {
                    return String.format("(%s%s)::timestamptz AT time zone 'asia/kolkata' as \"%s\"", obsColumn, column.getTextExtractor(), columnName);
                }
                case Time: {
                    return String.format("(%s%s)::TIME as \"%s\"", obsColumn, column.getTextExtractor(), columnName);
                }
                case Numeric: {
                    return String.format("(%s%s)::NUMERIC as \"%s\"", obsColumn, column.getTextExtractor(), columnName);
                }
                case Subject:
                case Encounter:
                case Location: {
                    // These three store the referenced record's UUID. Copied across as stored, the
                    // reporting column is unreadable and cannot be joined to anything, so resolve it
                    // to the name, the way get_coded_string_value resolves a coded answer above.
                    return String.format("public.get_reference_string_value(%s%s, '%s')::TEXT as \"%s\"",
                            obsColumn, column.getJsonbExtractor(), column.getConceptType(), columnName);
                }
                case ImageV2: {
                    return String.format("(%s%s)::JSONB as \"%s\"", obsColumn, column.getTextExtractor(), columnName);
                }
                default: {
                    return String.format("(%s%s)::TEXT as \"%s\"", obsColumn, column.getTextExtractor(), columnName);
                }
            }
        }).collect(Collectors.joining(",\n"));
        return "," + columnSelects;
    }
}
