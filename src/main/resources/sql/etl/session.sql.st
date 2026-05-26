INSERT INTO "<schemaName>"."<tableName>" (id, uuid, is_voided, created_by_id, last_modified_by_id,
                                          created_date_time, last_modified_date_time, organisation_id,
                                          group_subject_uuid, scheduled_date, attendance_type_uuid, status,
                                          reason_concept_uuid, notes, marked_by_user_id, marked_at)
SELECT entity.id,
       entity.uuid,
       entity.is_voided,
       entity.created_by_id,
       entity.last_modified_by_id,
       entity.created_date_time,
       entity.last_modified_date_time,
       entity.organisation_id,
       grp.uuid AS group_subject_uuid,
       entity.scheduled_date,
       at.uuid  AS attendance_type_uuid,
       entity.status,
       rc.uuid  AS reason_concept_uuid,
       entity.notes,
       entity.marked_by_user_id,
       entity.marked_at
FROM public.session entity
         JOIN public.individual grp ON grp.id = entity.group_subject_id
         JOIN public.attendance_type at ON at.id = entity.attendance_type_id
         LEFT JOIN public.concept rc ON rc.id = entity.reason_concept_id
WHERE entity.last_modified_date_time > '<startTime>'
  AND entity.last_modified_date_time \<= '<endTime>';
