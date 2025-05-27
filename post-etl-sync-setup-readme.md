# Post-ETL Sync Processing

## Overview

The Post-ETL Sync Processing feature enables custom data transformations after the standard ETL process is completed. This functionality allows organizations to create derived tables, perform complex data aggregations, and implement custom business logic on the ETL data.

## Key Features

- **Configurable Transformations**: Define custom SQL transformations through JSON configuration files
- **Incremental Processing**: Only process data that has changed since the last sync
- **Ordered Execution**: Execute DDL and DML operations in a specified order
- **Schema-specific Configurations**: Support for organization-specific transformations
- **Transaction Management**: Ensures data integrity during the transformation process

## Implementation Details

### Key Components

1. **PostETLConfig**: Domain model for the configuration structure
2. **PostETLSyncService**: Service that orchestrates the execution of post-ETL sync scripts
3. **PostETLSyncStatusRepository**: Manages the sync state and cutoff timestamps
4. **EtlService**: Integration point to trigger post-ETL sync after the main ETL process

### Execution Flow

1. The ETL process completes
2. PostETLSyncService loads the organization-specific configuration
3. DDL scripts are executed in order (if tables don't exist)
4. DML scripts are executed in order, with inserts followed by updates
5. The cutoff timestamp is updated for the next run

## Configuration and Usage Guidelines

### Configuration Structure

The Post-ETL Sync process is driven by a JSON configuration file named `post-etl-sync-processing-config.json` located in the organization-specific directory under `src/main/resources/post-etl/{organization}/`.

```json
{
  "ddl": [
    {
      "order": 1,
      "table": "table_name",
      "sql": "create-table-script.sql",
      "exists_check": "optional_custom_check_query"
    }
  ],
  "dml": [
    {
      "order": 1,
      "table": "target_table_name",
      "sqls": [
        {
          "order": 1,
          "sourceTableName": "source_table",
          "insert-sql": "insert-script.sql",
          "update-sqls": [
            "update-script-1.sql",
            "update-script-2.sql"
          ],
          "sql-params": [
            "optional_param1",
            "optional_param2"
          ]
        }
      ]
    }
  ]
}
```

### Configuration Requirements

1. **DDL Configuration**:
   - `order`: Numeric execution order (starts with 1)
   - `table`: Target table name
   - `sql`: SQL script filename for table creation
   - Optional: `exists_check` for custom table existence verification

2. **DML Configuration**:
   - `order`: Numeric execution order
   - `table`: Target table name
   - `sqls`: Array of source table operations with:
     - `order`: Execution order within the DML operation
     - `sourceTableName`: Source table for data
     - `insert-sql`: Script filename for insert operations (can be empty if only updates)
     - `update-sqls`: Array of update script filenames (can be empty if only inserts)
     - Optional: `sql-params` for additional parameters

### SQL Scripts

SQL scripts are stored in the organization-specific directory and referenced in the configuration file:

1. **DDL Scripts**: Create tables, indexes, and other database objects. These run first and only if the table doesn't already exist.

2. **DML Scripts**: Insert and update data in the tables:
   - **Insert SQL**: Adds new records to the target table
   - **Update SQLs**: Updates existing records in the target table

### Parameter Substitution

The system automatically replaces the following parameters in SQL scripts:

- `:previousCutoffDateTime`: Timestamp of the last successful sync
- `:newCutoffDateTime`: Current timestamp for this sync
- `:param1`, `:param2`, etc.: Custom parameters specified in the `sql-params` array

## Best Practices

### Schema Name Usage

1. **Always qualify table names** with the schema name in all SQL scripts:
   ```sql
   apfodisha.individual_child_growth_monitoring_report
   ```

2. **Begin DDL scripts with role setting** to ensure proper permissions:
   ```sql
   set role apfodisha;
   ```

3. **Use consistent schema names** throughout all related SQL scripts, matching the directory name under `post-etl/`

### Timestamp Filtering

1. **Always use both timestamp parameters** in SQL scripts that modify data:
   ```sql
   WHERE (column_datetime > :previousCutoffDateTime AND column_datetime <= :newCutoffDateTime)
   ```

2. **Apply filters to multiple timestamp columns** when applicable:
   ```sql
   WHERE (created_date_time > :previousCutoffDateTime AND created_date_time <= :newCutoffDateTime)
      OR (last_modified_date_time > :previousCutoffDateTime AND last_modified_date_time <= :newCutoffDateTime)
   ```

3. **Include timestamp filters in all subqueries and CTEs**:
   ```sql
   AND follow_up.last_modified_date_time > :previousCutoffDateTime 
   AND follow_up.last_modified_date_time <= :newCutoffDateTime
   ```

### SQL Script Practices

1. **Use descriptive prefixes** related to the target table (e.g., `icgmr-` for individual_child_growth_monitoring_report)

2. **Use CTEs for complex updates** and include explicit JOINs with proper conditions

3. **Always include `is_voided = false` checks** when applicable

4. **Specify data types and nullability** explicitly in CREATE TABLE statements

### Example SQL Script

```sql
-- Insert script
INSERT INTO schemaname.custom_report_table (field1, field2, field3)
SELECT 
    s.field1,
    s.field2,
    s.field3
FROM schemaname.source_table s
WHERE (s.created_date_time > :previousCutoffDateTime AND s.created_date_time <= :newCutoffDateTime)
   OR (s.last_modified_date_time > :previousCutoffDateTime AND s.last_modified_date_time <= :newCutoffDateTime);
```

## Adding New Transformations

To add new post-ETL transformations:

1. Create SQL scripts for table creation and data manipulation
2. Update the organization's `post-etl-sync-processing-config.json` file
3. Place all files in the organization-specific directory under `src/main/resources/post-etl/{organization}/`

## Current Implementations

The system currently implements transformations for:

- **APF Odisha**: Child growth monitoring reports with derived fields for nutrition status
- **RWB**: Custom reporting tables

## Troubleshooting and Security

### Delete previous version of specific post_etl_sync_processing_config database table. An example is shown below

```sql
DELETE FROM public.post_etl_sync_status WHERE db_user = 'apfodisha';
DROP TABLE apfodisha.individual_child_growth_monitoring_report;
```

### Troubleshooting

- Check application logs for detailed execution information
- Verify SQL scripts use the correct schema and table names
- Ensure parameter placeholders match the expected format
- Confirm the configuration file follows the correct JSON structure

### Security Considerations

- SQL scripts run with the organization's database role
- Each organization's transformations are isolated to their own schema
- The search path is reset after execution to prevent cross-schema access
