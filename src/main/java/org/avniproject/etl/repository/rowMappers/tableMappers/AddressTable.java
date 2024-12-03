package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.repository.rowMappers.ColumnMetadataMapper;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class AddressTable extends Table {

    private final List<Map<String, Object>> addressLevelTypes;
    private final List<Map<String, Object>> formColumns;

    public AddressTable(List<Map<String, Object>> addressLevelTypes, List<Map<String, Object>> formColumns) {
        this.addressLevelTypes = addressLevelTypes;
        this.formColumns = formColumns;
    }

    @Override
    public String name(Map<String, Object> tableDetails) {
        return "address";
    }

    @Override
    public List<Column> columns() {
        List<Column> columns = this.addressLevelTypes
                .stream()
                .map(addressLevelTypeMap -> new Column((String) addressLevelTypeMap.get("name"), Column.Type.text))
                .collect(Collectors.toList());
        List<Column> idColumns = columns.stream().map(c -> new Column(format("%s id", c.getName()), Column.Type.integer, Column.ColumnType.index))
                .collect(Collectors.toList());
        return new Columns()
                .withColumns(columns)
                .withColumn(new Column("gps_coordinates", Column.Type.point))
                .withColumns(idColumns)
                .withIdColumn()
                .withCommonColumns()
                .build();
    }

    public List<ColumnMetadata> columnMetadata() {
        List<ColumnMetadata> columnMetadata = this.columns().stream()
                .map(column -> new ColumnMetadata(column, null, null, null, false))
                .collect(Collectors.toList());
        List<ColumnMetadata> formColumnsMetadata = this.formColumns.stream()
                .filter(stringObjectMap -> stringObjectMap.get("concept_id") != null)
                .map(column -> new ColumnMetadataMapper().create(column)).toList();
        columnMetadata.addAll(formColumnsMetadata);
        return columnMetadata;
    }
}
