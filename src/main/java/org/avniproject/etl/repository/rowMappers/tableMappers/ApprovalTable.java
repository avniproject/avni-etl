package org.avniproject.etl.repository.rowMappers.tableMappers;

import org.avniproject.etl.domain.metadata.Column;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * One row per approval decision, with the answers given on the Approval form as columns (#174).
 *
 * One row per decision, not per record: a record judged more than once appears more than once, which is
 * what lets a report count rejections by reason over time. Nothing here may assume one row per entity.
 */
public class ApprovalTable extends Table {

    @Override
    public List<Column> columns() {
        return new Columns()
                .withIdColumn()
                .withCommonColumns()
                .withColumns(Arrays.asList(
                                new Column("individual_id", Column.Type.integer, Column.ColumnType.index),
                                new Column("address_id", Column.Type.integer, Column.ColumnType.index),
                                // The record this decision was about. Joins to the parent table's id.
                                new Column("entity_id", Column.Type.integer, Column.ColumnType.index),
                                new Column("entity_type", Column.Type.text),
                                new Column("entity_type_uuid", Column.Type.text),
                                new Column("approval_status", Column.Type.text, Column.ColumnType.index),
                                new Column("status_date_time", Column.Type.timestampWithTimezone, Column.ColumnType.index),
                                new Column("approval_status_comment", Column.Type.text),
                                new Column("auto_approved", Column.Type.bool)
                        )
                ).build();
    }

    /**
     * The suffix is load-bearing, not decoration. generateTableName's first argument is only the key into
     * TableNameGenerator.trims - the name itself is built from the parts plus the suffix. Without
     * "APPROVAL" this resolves to exactly the string EncounterTable produces for the same subject and
     * encounter type, and CreateTable.getSql() opens with "drop table if exists <name> cascade", so the
     * org's encounter table and its dependent views would be dropped rather than an error raised.
     *
     * All three parts are passed because these are the first form types with a variable mapping shape;
     * omitting the programme would collide the subject-only and subject+programme mappings, which
     * TableMetadata.matches() keeps as two distinct tables.
     */
    @Override
    public String name(Map<String, Object> tableDetails) {
        return generateTableName("Approval", "APPROVAL", tableDetails,
                "subject_type_name", "program_name", "encounter_type_name");
    }
}
