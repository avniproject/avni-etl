package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.Model;
import org.avniproject.etl.domain.metadata.Column.Type;
import org.avniproject.etl.domain.metadata.diff.Diff;
import org.avniproject.etl.domain.metadata.diff.RenameColumn;

import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static org.springframework.util.ObjectUtils.nullSafeEquals;

public class ColumnMetadata extends Model {
    private final Column column;
    private final Integer conceptId;
    private final ConceptType conceptType;
    private final String conceptUuid;
    private final String parentConceptUuid;
    private final boolean conceptVoided;

    public static String getVoidedName(String name) {
        return "voided_" + name;
    }

    public String getVoidedName() {
        return getVoidedName(getName());
    }

    public enum ConceptType {
        Audio,
        Date,
        DateTime,
        Duration,
        File,
        GroupAffiliation,
        Id,
        Image,
        Location,
        MultiSelect,
        NA,
        Notes,
        Numeric,
        PhoneNumber,
        SingleSelect,
        Subject,
        Encounter,
        Text,
        Time,
        QuestionGroup,
        Video;

        public Column.Type getColumnDatatype() {
            switch (this) {
                case Numeric:
                    return Column.Type.numeric;
                case Date:
                    return Column.Type.date;
                case DateTime:
                    return Column.Type.timestamp;
                case Time:
                    return Column.Type.time;
                default:
                    return Column.Type.text;
            }
        }


    }

    public ColumnMetadata(Integer id, Column column, Integer conceptId, ConceptType conceptType, String conceptUuid, String parentConceptUuid, boolean conceptVoided) {
        super(id);
        this.column = column;
        this.conceptId = conceptId;
        this.conceptType = conceptType;
        this.conceptUuid = conceptUuid;
        this.parentConceptUuid = parentConceptUuid;
        this.conceptVoided = conceptVoided;
    }

    public boolean isConceptVoided() {
        return conceptVoided;
    }

    public ColumnMetadata(Integer id, String name, Integer conceptId, ConceptType conceptType, String conceptUuid, String parentConceptUuid, Column.ColumnType columnType, boolean conceptVoided) {
        this(id, conceptType == null ? new Column(name, null, columnType) : new Column(name, conceptType.getColumnDatatype(), columnType), conceptId, conceptType, conceptUuid, parentConceptUuid, conceptVoided);
    }

    public ColumnMetadata(Column column, Integer conceptId, ConceptType conceptType, String conceptUuid, boolean conceptVoided) {
        this(null, column, conceptId, conceptType, conceptUuid, null, conceptVoided);
    }

    public Integer getConceptId() {
        return conceptId;
    }

    public ConceptType getConceptType() {
        return conceptType;
    }

    public Column getColumn() {
        return this.column;
    }

    public String getName() {
        return column.getName();
    }

    public Type getType() {
        return column.getType();
    }

    public String getConceptUuid() {
        return conceptUuid;
    }

    public String getParentConceptUuid() {
        return parentConceptUuid;
    }

    public boolean matches(ColumnMetadata realColumn) {
        if (realColumn == null) return false;
        if (realColumn.getConceptUuid() == null && getConceptUuid() == null) {
            return getName().equals(realColumn.getName());
        }
        if (realColumn.getParentConceptUuid() != null || getParentConceptUuid() != null) {
            return nullSafeEquals(realColumn.getParentConceptUuid(), getParentConceptUuid()) &&
                    nullSafeEquals(realColumn.getConceptUuid(), getConceptUuid());
        }
        return nullSafeEquals(realColumn.getConceptUuid(), getConceptUuid());
    }

    public List<Diff> findChanges(TableMetadata newTable, ColumnMetadata oldColumnMetadata) {
        if (!getName().equals(oldColumnMetadata.getName())) {
            return List.of(new RenameColumn(newTable.getName(), oldColumnMetadata.getName(), getName()));
        }
        if (!getType().equals(oldColumnMetadata.getType())) {
            throw new RuntimeException(String.format("Change in datatype detected. Table: %s, Column: %s, Old Type: %s, New Type: %s", newTable.getName(), getName(), getType(), oldColumnMetadata.getType()));
        }
        return Collections.emptyList();
    }

    public void mergeWith(ColumnMetadata oldColumnMetadata) {
        setId(oldColumnMetadata.getId());
    }

    public String getJsonbExtractor() {
        if (parentConceptUuid != null) {
            return format("-> '%s' -> '%s'", parentConceptUuid, conceptUuid);
        }
        return format("-> '%s'", conceptUuid);
    }

    public String getTextExtractor() {
        if (parentConceptUuid != null) {
            return format("-> '%s' ->> '%s'", parentConceptUuid, conceptUuid);
        }
        return format("->> '%s'", conceptUuid);
    }

    @Override
    public String toString() {
        return "{" +
                "column=" + column +
                '}';
    }

    public ColumnMetadata getNewVoidedColumnMetaData() {
        String newName = this.conceptVoided ? this.getName() : this.getVoidedName();
        Column newColumn = this.getColumn().getClonedColumn(newName);

        return new ColumnMetadata(this.getId(), newColumn, this.getConceptId(), this.getConceptType(), this.getConceptUuid(), this.getParentConceptUuid(), true);
    }
}
