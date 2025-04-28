-- ETL Re-run

-- Find routines with 'delete' string in their name
SELECT routines.routine_name, parameters.data_type, parameters.ordinal_position
FROM information_schema.routines
         LEFT JOIN information_schema.parameters ON routines.specific_name=parameters.specific_name
WHERE routines.specific_schema='public' and routines.routine_name like '%delete%'
ORDER BY routines.routine_name, parameters.ordinal_position;

-- Show content of routine with name 'delete_etl_metadata_for_schema'
select * from information_schema.routines where routines.routine_name= 'delete_etl_metadata_for_schema';
select * from pg_proc where proname= 'delete_etl_metadata_for_schema';

-- Create or replace function to delete etl metadata for an org
DROP FUNCTION IF EXISTS delete_etl_metadata_for_schema;
create function delete_etl_metadata_for_schema(in_impl_schema text, in_db_user text, in_db_owner text) returns bool
    language plpgsql
as
$$
BEGIN
    execute 'set role "' || in_db_owner || '";';
    execute 'drop schema if exists "' || in_impl_schema || '" cascade;';
    execute 'delete from entity_sync_status where db_user = ''' || in_db_user || ''';';
    execute 'delete from entity_sync_status where schema_name = ''' || in_impl_schema || ''';';
    execute 'delete from index_metadata where table_metadata_id in (select id from table_metadata where schema_name = ''' || in_impl_schema || ''');';
    execute 'delete from column_metadata where table_id in (select id from table_metadata where schema_name = ''' || in_impl_schema || ''');';
    execute 'delete from table_metadata where schema_name = ''' || in_impl_schema || ''';';
    return true;
END
$$;

-- Create function to delete etl metadata for org-group
DROP FUNCTION IF EXISTS delete_etl_metadata_for_org;
create function delete_etl_metadata_for_org(in_impl_schema text, in_db_user text) returns bool
    language plpgsql
as
$$
BEGIN
    EXECUTE 'set role openchs;';
    execute 'drop schema "' || in_impl_schema || '" cascade;';
    execute 'delete from entity_sync_status where db_user = ''' || in_db_user || ''';';
    execute 'delete from entity_sync_status where schema_name = ''' || in_impl_schema || ''';';
    execute 'delete from index_metadata where table_metadata_id in (select id from table_metadata where schema_name = ''' || in_impl_schema || ''');';
    execute 'delete from column_metadata where table_id in (select id from table_metadata where schema_name = ''' || in_impl_schema || ''');';
    execute 'delete from table_metadata where schema_name = ''' || in_impl_schema || ''';';
    return true;
END
$$;

-- List all waiting triggers for ETL , in ascending order of next trigger
select
    qt.trigger_state,
    qt.trigger_type,
    qjd.job_name,
    qjd.sched_name,
    qjd.job_group,
    qjd.description,
    qt.next_fire_time
from public.qrtz_triggers qt
         join public.qrtz_job_details qjd
              on qt.sched_name = qjd.sched_name
                  and qt.job_name = qjd.job_name
                  and qt.job_group = qjd.job_group
order by qt.next_fire_time asc;

-- List currently running ETL job
select qft.state,
       qt.trigger_state,
       qt.trigger_type,
       qjd.job_name,
       qjd.sched_name,
       qjd.job_group,
       qjd.description
from qrtz_fired_triggers qft
         join qrtz_triggers qt on qft.job_name = qt.job_name
         join qrtz_job_details qjd on qt.sched_name = qjd.sched_name and qt.job_name = qjd.job_name and qt.job_group = qjd.job_group;

-- Query to list org-uuids and org-group-uuids with ETL enabled (SyncJob)
select job_name
from (select uuid organisationUUID from organisation) foo
        right join qrtz_job_details qjd on organisationUUID = qjd.job_name
where qjd.job_group = 'SyncJobs'
   or qjd is null;

-- SQL Function to Cleanup ETL Data for a Table in a Schema

CREATE OR REPLACE FUNCTION cleanup_etl_data(p_schema_name TEXT, p_table_name TEXT)
    RETURNS VOID AS
$$
DECLARE
    v_table_id INT;
    v_drop_table_sql TEXT;
BEGIN
    -- Get the table ID
    SELECT id INTO v_table_id
    FROM public.table_metadata
    WHERE schema_name = p_schema_name AND name = p_table_name;

    IF v_table_id IS NULL THEN
        RAISE EXCEPTION 'Table metadata not found for schema: %, table: %', p_schema_name, p_table_name;
    END IF;

    -- Delete from index_metadata
    DELETE FROM public.index_metadata
    WHERE column_id IN (
        SELECT id FROM public.column_metadata WHERE table_id = v_table_id
    );

    -- Delete from entity_sync_status
    DELETE FROM public.entity_sync_status
    WHERE table_metadata_id = v_table_id;

    -- Delete from column_metadata
    DELETE FROM public.column_metadata
    WHERE table_id = v_table_id;

    -- Delete from table_metadata
    DELETE FROM public.table_metadata
    WHERE id = v_table_id;

    -- Drop the actual table
    v_drop_table_sql := FORMAT('DROP TABLE IF EXISTS %I.%I', p_schema_name, p_table_name);
    EXECUTE v_drop_table_sql;

    RAISE NOTICE 'Cleanup completed for %.%', p_schema_name, p_table_name;
END;
$$ LANGUAGE plpgsql;

-- ### How to Use the above Function
-- reset role;
-- select * from apfodishauat.individual_child_qrt_child; -- table is present
-- SELECT cleanup_etl_data('apfodishauat', 'individual_child_qrt_child');
-- select * from apfodishauat.individual_child_qrt_child; -- table is not found anymore
