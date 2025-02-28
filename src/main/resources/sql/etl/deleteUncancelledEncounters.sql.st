delete from "<schemaName>".<encounterCancelTableName>
using "<schemaName>".<primaryTableName>
where "<schemaName>".<encounterCancelTableName>.id = "<schemaName>".<primaryTableName>.id
and "<schemaName>".<primaryTableName>.cancel_date_time is null;