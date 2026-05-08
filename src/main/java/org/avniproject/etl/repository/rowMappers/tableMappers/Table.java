package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.repository.rowMappers.TableNameGenerator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

abstract public class Table {
    abstract public String name(Map<String, Object> tableDetails);
    abstract public List<Column> columns();

    protected String generateTableName(String tableType, String suffix, Map<String, Object> tableDetails, String... partKeys) {
        TableNameGenerator tableNameGenerator = new TableNameGenerator();
        List<String> parts = Arrays.stream(partKeys).map(s -> (String) tableDetails.get(s)).collect(Collectors.toList());
        return tableNameGenerator.generateName(parts, tableType, suffix);
    }
}
