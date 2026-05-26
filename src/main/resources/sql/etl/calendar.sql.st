INSERT INTO "<schemaName>"."<tableName>" (id, uuid, is_voided, created_by_id, last_modified_by_id,
                                          created_date_time, last_modified_date_time, organisation_id,
                                          name, working_pattern, address_level_uuid, is_default)
SELECT entity.id,
       entity.uuid,
       entity.is_voided,
       entity.created_by_id,
       entity.last_modified_by_id,
       entity.created_date_time,
       entity.last_modified_date_time,
       entity.organisation_id,
       entity.name,
       entity.working_pattern,
       al.uuid AS address_level_uuid,
       entity.is_default
FROM public.calendar entity
         LEFT JOIN public.address_level al ON al.id = entity.address_level_id
WHERE entity.last_modified_date_time > '<startTime>'
  AND entity.last_modified_date_time \<= '<endTime>';
