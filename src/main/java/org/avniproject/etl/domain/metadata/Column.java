package org.avniproject.etl.domain.metadata;

public class Column {
    private static final int POSTGRES_MAX_COLUMN_NAME_LENGTH = 63;
    private static final int NUMBER_OF_CHARACTERS_TO_ACCOMMODATE_HASHCODE = 14;

    private final String name;
    private final Type type;
    private final ColumnType columnType;

    public Column(String name, Type type) {
        this.name = getName(name);
        this.type = type;
        this.columnType = null;
    }

    public Column(String name, Type type, ColumnType columnType) {
        this.name = getName(name);
        this.type = type;
        this.columnType = columnType;
    }

    public String getName() {
        return this.name;
    }

    private String getTruncatedName(String name) {
        byte[] truncatedNameBytes = new byte[POSTGRES_MAX_COLUMN_NAME_LENGTH - NUMBER_OF_CHARACTERS_TO_ACCOMMODATE_HASHCODE];
        System.arraycopy(name.getBytes(), 0, truncatedNameBytes, 0, truncatedNameBytes.length);
        return String.format("%s (%s)", new String(truncatedNameBytes), Math.abs(name.hashCode()));
    }

    private String getName(String name) {
        name = name.replace("\"", "");
        if (isColumnNameTruncated(name)) {
            String truncatedName = getTruncatedName(name);
            while (isColumnNameTruncated(truncatedName)) {
                truncatedName = getTruncatedName(truncatedName);
            }
            return truncatedName;
        }
        return name;
    }

    private boolean isColumnNameTruncated(String name) {
        return name.getBytes().length > POSTGRES_MAX_COLUMN_NAME_LENGTH;
    }

    public Type getType() {
        return type;
    }

    public boolean isIndexed() {
        return ColumnType.index.equals(this.columnType);
    }

    public boolean isSyncAttributeColumn() {
        return ColumnType.syncAttribute1.equals(this.columnType) ||
                ColumnType.syncAttribute2.equals(this.columnType);
    }

    @Override
    public String toString() {
        return "{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", columnType=" + columnType +
                '}';
    }

    public enum ColumnType {
        index,
        syncAttribute1,
        syncAttribute2
    }

    public enum Type {
        serial,
        text,
        date,
        bool,
        timestamp,
        point,
        timestampWithTimezone,
        numeric,
        time,
        integer;

        public String typeString() {
            switch (this) {
                case serial:
                    return "serial";
                case integer:
                    return "integer";
                case numeric:
                    return "numeric";
                case date:
                    return "date";
                case time:
                    return "time";
                case text:
                    return "text";
                case bool:
                    return "boolean";
                case point:
                    return "point";
                case timestamp:
                    return "timestamp";
                case timestampWithTimezone:
                    return "timestamp with time zone";
                default:
                    throw new RuntimeException("column_name is not defined for this type"); //Not an expected scenario
            }
        }
    }
}
