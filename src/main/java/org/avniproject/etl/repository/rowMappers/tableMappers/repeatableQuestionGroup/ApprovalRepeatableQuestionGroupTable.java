package org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.repository.rowMappers.tableMappers.Columns;
import org.avniproject.etl.repository.rowMappers.tableMappers.Table;

import java.util.List;
import java.util.Map;

import static org.avniproject.etl.repository.rowMappers.TableNameGenerator.ApprovalRepeatableQuestionGroup;
import static org.avniproject.etl.repository.rowMappers.tableMappers.CommonColumns.CommonRepeatableGroupColumns;

/**
 * A repeatable question group asked on an Approval form (#174).
 *
 * The parent is the decision, not the record judged, so the child rows hang off
 * entity_approval_status_id. A record judged twice has two decisions and therefore two sets of
 * question-group rows - which is what lets a report count answers per decision rather than per record.
 *
 * The APPROVAL suffix is load-bearing, not decoration. generateTableName's first argument is only the
 * key into TableNameGenerator.trims; the name itself is built from the parts plus the suffix. Without it
 * a mapping on a subject type and a visit type resolves to exactly the string
 * EncounterRepeatableQuestionGroupTable produces for the same parts, and CreateTable.getSql() opens with
 * "drop table if exists <name> cascade" - so a collision drops that table and its dependent views rather
 * than raising an error.
 *
 * All four parts are passed because a decision form's mapping shape varies - subject only, +programme,
 * +visit type, or both - and TableNameGenerator drops absent parts.
 */
public class ApprovalRepeatableQuestionGroupTable extends Table {
    @Override
    public List<Column> columns() {
        return new Columns()
                .withColumns(CommonRepeatableGroupColumns)
                .withColumns(List.of(
                        new Column("individual_id", Column.Type.integer, Column.ColumnType.index),
                        new Column("address_id", Column.Type.integer, Column.ColumnType.index),
                        new Column("entity_approval_status_id", Column.Type.integer, Column.ColumnType.index)))
                .build();
    }

    @Override
    public String name(Map<String, Object> tableDetails) {
        return generateTableName(ApprovalRepeatableQuestionGroup, "APPROVAL", tableDetails,
                "subject_type_name", "program_name", "encounter_type_name", "parent_concept_name");
    }
}
