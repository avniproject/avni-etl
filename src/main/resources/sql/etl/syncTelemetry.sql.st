INSERT INTO "<schemaName>".<tableName> (id, uuid, user_id, organisation_id, created_by_id,
    last_modified_by_id, created_date_time, last_modified_date_time, sync_status,
   sync_start_time, sync_end_time, device_name, android_version, app_version, device_info, sync_source)

SELECT entity.id,
       entity.uuid,
       entity.user_id,
       entity.organisation_id,
       entity.created_by_id,
       entity.last_modified_by_id,
       entity.created_date_time,
       entity.last_modified_date_time,
       entity.sync_status,
       entity.sync_start_time,
       entity.sync_end_time,
       entity.device_name,
       entity.android_version,
       entity.app_version,
       entity.device_info,
       entity.sync_source

FROM public.sync_telemetry entity
where entity.last_modified_date_time > '<startTime>'
  and entity.last_modified_date_time \<= '<endTime>';