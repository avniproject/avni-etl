package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.metadata.diff.Diff;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Comparator;

import static java.util.stream.Collectors.groupingBy;

public class SchemaMetadata {
    private List<TableMetadata> tableMetadata;

    public SchemaMetadata(List<TableMetadata> tableMetadata) {
        this.tableMetadata = tableMetadata;
    }

    public List<TableMetadata> getTableMetadata() {
        return tableMetadata;
    }

    public List<TableMetadata> getOrderedTableMetadata() {
        List<TableMetadata> sortedTableMetadata = new ArrayList<>(tableMetadata);
        sortedTableMetadata.sort(Comparator.comparing(TableMetadata::getType));
        return sortedTableMetadata;
    }

    public void setTableMetadata(List<TableMetadata> tableMetadata) {
        this.tableMetadata = tableMetadata;
    }

    public List<Diff> findChanges(SchemaMetadata current) {
        List<TableMetadata> newTables = this.getTableMetadata();
        List<Diff> diffs = new ArrayList<>();
        newTables.forEach(newTable -> {
            diffs.addAll(findChanges(current, newTable));
        });
        return diffs;
    }

    public Optional<TableMetadata> findMatchingTable(TableMetadata newTable) {
        return this.getTableMetadata().stream().filter(currentTable -> currentTable.matches(newTable)).findFirst();
    }

    public List<TableMetadata> getAllSubjectTables() {
        return tableMetadata.stream().filter(TableMetadata::isSubjectTable).toList();
    }

    public List<String> getAllSubjectTableNames() {
        List<TableMetadata> subjectTables = getAllSubjectTables();
        List<String> subjectTableNames = new ArrayList<>();
        for(TableMetadata subject : subjectTables){
            subjectTableNames.add(subject.getName());
        }
        return subjectTableNames;
    }

    public List<TableMetadata> getAllProgramEnrolmentTables() {
        return tableMetadata.stream().filter(table -> table.getType() == TableMetadata.Type.ProgramEnrolment).toList();
    }

    public List<String> getAllProgramEnrolmentTableNames() {
        List<TableMetadata> programEnrolmentTables = getAllProgramEnrolmentTables();
        List<String> programEnrolmentTableNames = new ArrayList<>();
        for(TableMetadata programEnrolment : programEnrolmentTables){
            programEnrolmentTableNames.add(programEnrolment.getName());
        }
        return programEnrolmentTableNames;
    }

    public List<TableMetadata> getAllProgramEncounterTables() {
        return tableMetadata.stream().filter(table -> table.getType() == TableMetadata.Type.ProgramEncounter).toList();
    }

    public List<String> getAllProgramEncounterTableNames() {
        List<TableMetadata> programEncounterTables = getAllProgramEncounterTables();
        List<String> programEncounterTableNames = new ArrayList<>();
        for(TableMetadata programEncounter : programEncounterTables){
            programEncounterTableNames.add(programEncounter.getName());
        }
        return programEncounterTableNames;
    }

    public List<TableMetadata> getAllEncounterTables() {
        return tableMetadata.stream().filter(table -> table.getType() == TableMetadata.Type.Encounter).toList();
    }

    public List<TableMetadata> getAllIndividualAndProgramEncounterTables() {
        return tableMetadata.stream().filter(table -> table.getType() == TableMetadata.Type.Encounter
                || table.getType() == TableMetadata.Type.ProgramEncounter).toList();
    }

    public List<String> getAllEncounterTableNames() {
        List<TableMetadata> encounterTables = getAllEncounterTables();
        List<String> encounterTableNames = new ArrayList<>();
        for(TableMetadata encounter : encounterTables){
            encounterTableNames.add(encounter.getName());
        }
        return encounterTableNames;
    }

    public Optional<TableMetadata> getMediaTable() {
        return tableMetadata.stream().filter(TableMetadata::isMediaTable).findFirst();
    }

    public Optional<TableMetadata> getMediaAnalysisTable() {
        return tableMetadata.stream().filter(TableMetadata::isMediaAnalysisTable).findFirst();
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

    public String getCountsByType() {
        Map<TableMetadata.Type, List<TableMetadata>> grouped = this.tableMetadata.stream().collect(groupingBy(TableMetadata::getType));
        List<String> strings = grouped.entrySet().stream().map(x -> String.format("%s-%d", x.getKey(), x.getValue().size())).collect(Collectors.toList());
        return String.join(";", strings);
    }

    public Optional<TableMetadata> findTableByForm(String formUuid) {
        return this.tableMetadata.stream()
            .filter(table -> table.getFormUuid() != null && table.getFormUuid().equals(formUuid))
            .findFirst();
    }
}
