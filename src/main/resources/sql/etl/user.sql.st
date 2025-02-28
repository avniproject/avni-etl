INSERT INTO <schemaName>.<tableName> (id, uuid, username, organisation_id, created_by_id,
    last_modified_by_id, created_date_time, last_modified_date_time, catchment_id,
   email, phone_number, name, is_voided)
SELECT entity.id,
       entity.uuid,
       entity.username,
       entity.organisation_id,
       entity.created_by_id,
       entity.last_modified_by_id,
       entity.created_date_time,
       entity.last_modified_date_time,
       entity.catchment_id,
       entity.email,
       entity.phone_number,
       entity.name,
       entity.is_voided
FROM public.users entity
where entity.last_modified_date_time > '<startTime>'
  and entity.last_modified_date_time \<= '<endTime>';
