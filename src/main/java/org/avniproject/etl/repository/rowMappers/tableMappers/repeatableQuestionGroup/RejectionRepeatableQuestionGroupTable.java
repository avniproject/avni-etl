package org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup;

import org.avniproject.etl.domain.metadata.Column;
import org.avniproject.etl.repository.rowMappers.tableMappers.Columns;
import org.avniproject.etl.repository.rowMappers.tableMappers.Table;

import java.util.List;
import java.util.Map;

import static org.avniproject.etl.repository.rowMappers.TableNameGenerator.RejectionRepeatableQuestionGroup;
import static org.avniproject.etl.repository.rowMappers.tableMappers.CommonColumns.CommonRepeatableGroupColumns;

/**
 * A repeatable question group asked on a Rejection form (#174).
 *
 * Identical in shape to ApprovalRepeatableQuestionGroupTable and deliberately a separate table: a
 * rejection's answers are a different question set from an approval's, and a report counting reasons for
 * rejection must not have approvals mixed into it. The REJECTION suffix is what keeps the two apart, and
 * keeps both clear of the encounter question-group table - see ApprovalRepeatableQuestionGroupTable for
 * why a name collision is destructive rather than merely wrong.
 */
public class RejectionRepeatableQuestionGroupTable extends Table {
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
        return generateTableName(RejectionRepeatableQuestionGroup, "REJECTION", tableDetails,
                "subject_type_name", "program_name", "encounter_type_name", "parent_concept_name");
    }
}
