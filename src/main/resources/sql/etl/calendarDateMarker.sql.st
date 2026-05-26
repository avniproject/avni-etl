INSERT INTO "<schemaName>"."<tableName>" (id, uuid, is_voided, created_by_id, last_modified_by_id,
                                          created_date_time, last_modified_date_time, organisation_id,
                                          calendar_uuid, marker_date, name, is_working)
SELECT entity.id,
       entity.uuid,
       entity.is_voided,
       entity.created_by_id,
       entity.last_modified_by_id,
       entity.created_date_time,
       entity.last_modified_date_time,
       entity.organisation_id,
       c.uuid AS calendar_uuid,
       entity.marker_date,
       entity.name,
       entity.is_working
FROM public.calendar_date_marker entity
         JOIN public.calendar c ON c.id = entity.calendar_id
WHERE entity.last_modified_date_time > '<startTime>'
  AND entity.last_modified_date_time \<= '<endTime>';
