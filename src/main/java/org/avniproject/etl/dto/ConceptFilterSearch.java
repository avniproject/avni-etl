package org.avniproject.etl.dto;

import java.util.List;

public class ConceptFilterSearch {
    private String tableName;
    private String columnName;
    private List<String> columnValues;
    private String from;
    private String to;
    private boolean isNonStringValue;

    public ConceptFilterSearch(String tableName, String columnName, List<String> columnValues, String from, String to, boolean isNonStringValue) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnValues = columnValues;
        this.from = from;
        this.to = to;
        this.isNonStringValue = isNonStringValue;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public List<String> getColumnValues() {
        return columnValues;
    }

    public void setColumnValues(List<String> columnValues) {
        this.columnValues = columnValues;
    }

    @Override
    public String toString() {
        return "ConceptFilterSearch{" +
            "tableName='" + tableName + '\'' +
            ", columnName='" + columnName + '\'' +
            ", columnValues=" + columnValues +
            ", from=" + from +
            ", to=" + to +
            ", isNonStringValue=" + isNonStringValue +
            '}';
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public boolean getNonStringValue() {
        return isNonStringValue;
    }

    public void setNonStringValue(boolean nonStringValue) {
        isNonStringValue = nonStringValue;
    }
}
