package org.avniproject.etl.repository.rowMappers.tableMappers;

import java.util.Map;

/**
 * The rejection half of #174 - same columns as ApprovalTable, different rows. The two are separated by
 * the status filter in their SQL templates, so this table holds only rejections.
 */
public class RejectionTable extends ApprovalTable {
    @Override
    public String name(Map<String, Object> tableDetails) {
        return generateTableName("Rejection", "REJECTION", tableDetails,
                "subject_type_name", "program_name", "encounter_type_name");
    }
}
