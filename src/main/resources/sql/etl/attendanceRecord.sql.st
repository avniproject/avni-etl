INSERT INTO "<schemaName>"."<tableName>" (id, uuid, is_voided, created_by_id, last_modified_by_id,
                                          created_date_time, last_modified_date_time, organisation_id,
                                          session_uuid, subject_uuid, status, reason_concept_uuids,
                                          other_reason_text, needs_follow_up, follow_up_encounter_uuid)
SELECT entity.id,
       entity.uuid,
       entity.is_voided,
       entity.created_by_id,
       entity.last_modified_by_id,
       entity.created_date_time,
       entity.last_modified_date_time,
       entity.organisation_id,
       s.uuid   AS session_uuid,
       sub.uuid AS subject_uuid,
       entity.status,
       entity.reason_concept_uuids,
       entity.other_reason_text,
       entity.needs_follow_up,
       entity.follow_up_encounter_uuid
FROM public.attendance_record entity
         JOIN public.session s ON s.id = entity.session_id
         JOIN public.individual sub ON sub.id = entity.subject_id
WHERE entity.last_modified_date_time > '<startTime>'
  AND entity.last_modified_date_time \<= '<endTime>';
