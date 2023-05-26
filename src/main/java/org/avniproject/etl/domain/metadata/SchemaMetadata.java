package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.metadata.diff.Diff;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SchemaMetadata {
    private List<TableMetadata> tableMetadata;

    public SchemaMetadata(List<TableMetadata> tableMetadata) {
        this.tableMetadata = tableMetadata;
    }

    public List<TableMetadata> getTableMetadata() {
        return tableMetadata;
    }

    public void setTableMetadata(List<TableMetadata> tableMetadata) {
        this.tableMetadata = tableMetadata;
    }

    public List<Diff> findChanges(SchemaMetadata currentSchema) {
        List<TableMetadata> newTables = this.getTableMetadata();
        List<Diff> diffs = new ArrayList<>();
        newTables.forEach(newTable -> {
            diffs.addAll(findChanges(currentSchema, newTable));
        });
        return diffs;
    }

    public Optional<TableMetadata> findMatchingTable(TableMetadata newTable) {
        return this.getTableMetadata().stream().filter(currentTable -> currentTable.matches(newTable)).findFirst();
    }

    public Optional<ColumnMetadata> findMatchingColumn(TableMetadata newTable, ColumnMetadata columnMetadata) {
        Optional<TableMetadata> optionalTable = findMatchingTable(newTable);
        if (optionalTable.isPresent()) {
            TableMetadata table = optionalTable.get();
            return table.getColumnMetadataList().stream().filter(currentColumn -> currentColumn.matches(columnMetadata)).findFirst();
        }
        return Optional.empty();
    }

    public List<TableMetadata> getAllSubjectTables() {
        return tableMetadata.stream().filter(TableMetadata::isSubjectTable).toList();
    }

    public List<TableMetadata> getAllProgramEnrolmentTables() {
        return tableMetadata.stream().filter(table -> table.getType() == TableMetadata.Type.ProgramEnrolment).toList();
    }

    public List<TableMetadata> getAllProgramEncounterTables() {
        return tableMetadata.stream().filter(table -> table.getType() == TableMetadata.Type.ProgramEncounter).toList();
    }

    public List<TableMetadata> getAllEncounterTables() {
        return tableMetadata.stream().filter(table -> table.getType() == TableMetadata.Type.Encounter).toList();
    }

    private List<Diff> findChanges(SchemaMetadata currentSchema, TableMetadata newTable) {
        List<Diff> diffs = new ArrayList<>();
        Optional<TableMetadata> optionalMatchingTable = currentSchema.findMatchingTable(newTable);
        if (optionalMatchingTable.isPresent()) {
            TableMetadata matchingTable = optionalMatchingTable.get();
            diffs.addAll(newTable.findChanges(matchingTable));
        } else {
            diffs.addAll(newTable.createNew());
        }
        return diffs;
    }

    public void mergeWith(SchemaMetadata oldSchemaMetadata) {
        getTableMetadata()
                .forEach(newTable ->
                        oldSchemaMetadata
                                .findMatchingTable(newTable)
                                .ifPresent(newTable::mergeWith));
    }
}
