package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.Model;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.diff.*;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.util.ObjectUtils.nullSafeEquals;

public class TableMetadata extends Model {
    private String name;
    private Type type;
    private Type parentType;
    private String subjectTypeUuid;
    private String programUuid;
    private String encounterTypeUuid;
    private String formUuid;
    private String groupSubjectTypeUuid;
    private String memberSubjectTypeUuid;
    private String repeatableQuestionGroupConceptUuid;
    private List<ColumnMetadata> columnMetadataList = new ArrayList<>();
    private List<IndexMetadata> indexMetadataList = new ArrayList<>();

    public TableMetadata(Integer id) {
        super(id);
    }

    public TableMetadata() {
    }

    public boolean matches(TableMetadata other) {
        if (other == null) return false;
        return nullSafeEquals(other.getType(), this.getType())
                && nullSafeEquals(other.subjectTypeUuid, this.subjectTypeUuid)
                && nullSafeEquals(other.type, this.type)
                && nullSafeEquals(other.encounterTypeUuid, this.encounterTypeUuid)
                && nullSafeEquals(other.programUuid, this.programUuid)
                && nullSafeEquals(other.groupSubjectTypeUuid, this.groupSubjectTypeUuid)
                && nullSafeEquals(other.memberSubjectTypeUuid, this.memberSubjectTypeUuid)
                && nullSafeEquals(other.repeatableQuestionGroupConceptUuid, this.repeatableQuestionGroupConceptUuid);
    }

    public List<Diff> findChanges(TableMetadata existingTable) {
        ArrayList<Diff> diffs = new ArrayList<>();
        if (!existingTable.getName().equals(getName())) {
            diffs.add(new RenameTable(existingTable.getName(), getName()));
        }

        diffs.addAll(getNowVoidedColumns(existingTable));

        ArrayList<Diff> columnDiffs = new ArrayList<>();
        getColumnMetadataList().forEach(columnMetadata -> {
            Optional<ColumnMetadata> matchingColumn = existingTable.findMatchingColumn(columnMetadata);
            if (matchingColumn.isEmpty()) {
                columnDiffs.add(new AddColumn(getName(), columnMetadata.getColumn()));
            } else {
                columnDiffs.addAll(columnMetadata.findChanges(this, matchingColumn.get()));
            }
        });
        diffs.addAll(columnDiffs.stream().filter(diff -> diff instanceof RenameColumn).toList());
        diffs.addAll(columnDiffs.stream().filter(diff -> diff instanceof AddColumn).toList());

        getIndexMetadataList().forEach(indexMetadata -> {
            Optional<IndexMetadata> matchingIndex = existingTable.findMatchingIndex(indexMetadata);
            if (matchingIndex.isEmpty()) {
                diffs.add(indexMetadata.createIndex(getName()));
            }
        });

        return diffs;
    }

    private List<Diff> getNowVoidedColumns(TableMetadata existingTable) {
        List<Diff> diffs = new ArrayList<>();
        existingTable.getColumnMetadataList().forEach(existingColumn -> {
            Optional<ColumnMetadata> matchingColumn = findMatchingColumn(existingColumn);
            if (matchingColumn.isEmpty() && !existingColumn.isConceptVoided()) {
                OrganisationIdentity organisationIdentity = OrgIdentityContextHolder.getOrganisationIdentity();
                if (!organisationIdentity.isPartOfGroup())
                    diffs.add(new RenameColumn(getName(), existingColumn.getName(), existingColumn.getVoidedName()));
            }
        });
        return diffs;
    }

    private Optional<IndexMetadata> findMatchingIndex(IndexMetadata indexMetadata) {
        return this.indexMetadataList
                .stream()
                .filter(index -> index.matches(indexMetadata))
                .findFirst();
    }

    private Optional<ColumnMetadata> findMatchingColumn(ColumnMetadata columnMetadata) {
        return this.columnMetadataList
                .stream()
                .filter(thisColumn -> thisColumn.matches(columnMetadata))
                .findFirst();
    }

    public List<Column> getColumns() {
        return getColumnMetadataList()
                .stream()
                .map(ColumnMetadata::getColumn)
                .collect(Collectors.toList());
    }

    public List<ColumnMetadata> findColumnsMatchingConceptType(ColumnMetadata.ConceptType... conceptTypes) {
        List<ColumnMetadata> matchingColumns = this.getColumnMetadataList().stream()
            .filter(columnMetadata -> Arrays.stream(conceptTypes).anyMatch(conceptType -> 
                nullSafeEquals(columnMetadata.getConceptType(), conceptType)))
            .collect(Collectors.toList());
            
        return matchingColumns;
    }

    public void mergeWith(TableMetadata oldTableMetadata) {
        setId(oldTableMetadata.getId());
        getColumnMetadataList()
                .forEach(newColumn ->
                        oldTableMetadata
                                .findMatchingColumn(newColumn)
                                .ifPresent(newColumn::mergeWith));
        if (!OrgIdentityContextHolder.getOrganisationIdentity().isPartOfGroup()) {
            oldTableMetadata.getColumnMetadataList()
                    .stream().filter(oldColumnMetaData -> this.findMatchingColumn(oldColumnMetaData).isEmpty())
                    .forEach(missingInNewColumnMetaData -> this.columnMetadataList.add(missingInNewColumnMetaData.getNewVoidedColumnMetaData()));
        }
        getIndexMetadataList()
                .forEach(newIndex ->
                        oldTableMetadata.findMatchingIndex(newIndex)
                                .ifPresent(newIndex::mergeWith)
                );

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Type getParentType() {
        return parentType;
    }

    public void setParentType(Type parentType) {
        this.parentType = parentType;
    }

    public String getSubjectTypeUuid() {
        return subjectTypeUuid;
    }

    public void setSubjectTypeUuid(String subjectTypeUuid) {
        this.subjectTypeUuid = subjectTypeUuid;
    }

    public String getProgramUuid() {
        return programUuid;
    }

    public void setProgramUuid(String programUuid) {
        this.programUuid = programUuid;
    }

    public String getEncounterTypeUuid() {
        return encounterTypeUuid;
    }

    public void setEncounterTypeUuid(String encounterTypeUuid) {
        this.encounterTypeUuid = encounterTypeUuid;
    }

    public String getFormUuid() {
        return formUuid;
    }

    public void setFormUuid(String formUuid) {
        this.formUuid = formUuid;
    }

    public List<ColumnMetadata> getColumnMetadataList() {
        return columnMetadataList;
    }

    public void setColumnMetadataList(List<ColumnMetadata> columnMetadataList) {
        this.columnMetadataList = columnMetadataList;
    }

    public List<ColumnMetadata> getNonDefaultColumnMetadataList() {
        return getColumnMetadataList().stream().filter(columnMetadata -> columnMetadata.getConceptId() != null).collect(Collectors.toList());
    }

    public void addColumnMetadata(List<ColumnMetadata> columnMetadataList) {
        this.columnMetadataList.addAll(columnMetadataList);
    }

    public boolean hasNonDefaultColumns() {
        return !getNonDefaultColumnMetadataList().isEmpty();
    }

    public List<IndexMetadata> getIndexMetadataList() {
        return indexMetadataList;
    }

    public void addIndexMetadata(Column column) {
        addIndexMetadata(new IndexMetadata(findMatchingColumn(new ColumnMetadata(column, null, null, null, false)).get()));
    }

    public void addIndexMetadata(List<IndexMetadata> indexMetadataList) {
        this.indexMetadataList.addAll(indexMetadataList);
    }

    public void setIndexMetadataList(List<IndexMetadata> indexMetadataList) {
        this.indexMetadataList = indexMetadataList;
    }

    public String getGroupSubjectTypeUuid() {
        return groupSubjectTypeUuid;
    }

    public void setGroupSubjectTypeUuid(String groupSubjectTypeUuid) {
        this.groupSubjectTypeUuid = groupSubjectTypeUuid;
    }

    public String getMemberSubjectTypeUuid() {
        return memberSubjectTypeUuid;
    }

    public void setMemberSubjectTypeUuid(String memberSubjectTypeUuid) {
        this.memberSubjectTypeUuid = memberSubjectTypeUuid;
    }

    public List<Diff> createNew() {
        List<Diff> diffs = new ArrayList<>();
        diffs.add(new CreateTable(name, getColumns()));
        diffs.addAll(getIndexMetadataList()
                .stream()
                .map(indexMetadata -> new AddIndex(indexMetadata.getName(), getName(), indexMetadata.getColumnName()))
                .collect(Collectors.toList()));
        return diffs;
    }

    public boolean hasColumn(String columnName) {
        return columnMetadataList.stream().anyMatch(columnMetadata -> columnMetadata.getColumn().getName().equals(columnName));
    }

    public TableType getParentTableType() {
        if (!StringUtils.hasLength(repeatableQuestionGroupConceptUuid)) return null;

        if (StringUtils.hasLength(encounterTypeUuid))
            return StringUtils.hasLength(programUuid) ? TableType.ProgramEncounter : TableType.Encounter;
        if (StringUtils.hasLength(programUuid))
            return TableType.ProgramEnrolment;
        return TableType.IndividualProfile;
    }

    public ColumnMetadata getColumn(Integer columnId) {
        return columnMetadataList.stream().filter(columnMetadata -> columnMetadata.getId().equals(columnId)).findFirst().orElse(null);
    }

    public ColumnMetadata getColumnByConceptUuid(String conceptUuid) {
        return columnMetadataList.stream().filter(columnMetadata -> columnMetadata.getConceptUuid().equals(conceptUuid)).findFirst().orElse(null);
    }

    public Stream<ColumnMetadata> getAllColumnsByConceptUuid(String conceptUuid) {
        return columnMetadataList.stream().filter(columnMetadata -> columnMetadata.getConceptUuid() != null && columnMetadata.getConceptUuid().equals(conceptUuid));
    }

    public enum Type {
        Individual,
        Person,
        Household,
        Group,
        ProgramEnrolment,
        ProgramExit,
        ProgramEncounter,
        ProgramEncounterCancellation,
        Encounter,
        IndividualEncounterCancellation,
        Address,
        ManualProgramEnrolmentEligibility,
        GroupToMember,
        HouseholdToMember,
        RepeatableQuestionGroup,
        Checklist,
        SyncTelemetry,
        User,
        Media,
        MediaAnalysis
    }


    public enum TableType {
        IndividualProfile,
        Encounter,
        ProgramEnrolment,
        ProgramEncounter
    }

    public static final Map<TableType, String> qgParentColumnIds = Map.of(TableType.IndividualProfile, "individual_id",
            TableType.Encounter, "encounter_id",
            TableType.ProgramEnrolment, "program_enrolment_id",
            TableType.ProgramEncounter, "program_encounter_id");

    public boolean isSubjectTable() {
        return Arrays.asList(Type.Individual, Type.Person, Type.Household, Type.Group).contains(this.type);
    }

    public boolean isMediaTable() {
        return (Type.Media).equals(this.type);
    }

    public boolean isMediaAnalysisTable() {
        return (Type.MediaAnalysis).equals(this.type);
    }

    public boolean isPartOfRegularSync() {
        return !isMediaAnalysisTable();
    }

    private void addIndexMetadata(IndexMetadata indexMetadata) {
        this.indexMetadataList.add(indexMetadata);
    }

    public String getRepeatableQuestionGroupConceptUuid() {
        return repeatableQuestionGroupConceptUuid;
    }

    public void setRepeatableQuestionGroupConceptUuid(String repeatableQuestionGroupConceptUuid) {
        this.repeatableQuestionGroupConceptUuid = repeatableQuestionGroupConceptUuid;
    }

    public boolean isRepeatableQuestionGroupTable() {
        boolean isRqgType = getType() == TableMetadata.Type.RepeatableQuestionGroup;
        boolean hasConceptUuid = getRepeatableQuestionGroupConceptUuid() != null;
        return isRqgType && hasConceptUuid;
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", type=" + type +
                '}';
    }
}
