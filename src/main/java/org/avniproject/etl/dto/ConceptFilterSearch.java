package org.avniproject.etl.dto;

import java.util.List;

public class ConceptFilterSearch {
    private String tableName;
    private String columnName;
    private List<String> columnValues;

    public ConceptFilterSearch(String tableName, String columnName, List<String> columnValue) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnValues = columnValue;
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

    public List<String> getColumnValue() {
        return columnValues;
    }

    public void setColumnValue(List<String> columnValues) {
        this.columnValues = columnValues;
    }

    @Override
    public String toString() {
        return "ConceptFilterSearch{" +
            "tableName='" + tableName + '\'' +
            ", columnName='" + columnName + '\'' +
            ", columnValues=" + columnValues +
            '}';
    }
}
