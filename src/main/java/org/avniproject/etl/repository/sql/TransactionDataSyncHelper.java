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
                    return String.format("(%s%s) as \"%s\"", obsColumn, column.getTextExtractor(), columnName);
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
