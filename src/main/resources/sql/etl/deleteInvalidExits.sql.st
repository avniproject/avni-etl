delete from "<schemaName>".<exitTableName>
using "<schemaName>".<primaryTableName>
where "<schemaName>".<exitTableName>.id = "<schemaName>".<primaryTableName>.id
and "<schemaName>".<primaryTableName>.program_exit_date_time is null;