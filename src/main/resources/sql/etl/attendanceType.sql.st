INSERT INTO "<schemaName>"."<tableName>" (id, uuid, is_voided, created_by_id, last_modified_by_id,
                                          created_date_time, last_modified_date_time, organisation_id,
                                          subject_type_uuid, name, sort_order, config)
SELECT entity.id,
       entity.uuid,
       entity.is_voided,
       entity.created_by_id,
       entity.last_modified_by_id,
       entity.created_date_time,
       entity.last_modified_date_time,
       entity.organisation_id,
       st.uuid AS subject_type_uuid,
       entity.name,
       entity.sort_order,
       entity.config
FROM public.attendance_type entity
         JOIN public.subject_type st ON st.id = entity.subject_type_id
WHERE entity.last_modified_date_time > '<startTime>'
  AND entity.last_modified_date_time \<= '<endTime>';
