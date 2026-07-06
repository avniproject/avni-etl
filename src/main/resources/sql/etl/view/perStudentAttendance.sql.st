DROP MATERIALIZED VIEW IF EXISTS "<schema_name>"."<view_name>" CASCADE;
CREATE MATERIALIZED VIEW "<schema_name>"."<view_name>" AS
WITH windowed_expected AS (
    SELECT es.organisation_id, es.group_subject_uuid, es.scheduled_date,
           es.attendance_type_uuid, es.status AS session_status, es.calendar_day_type
    FROM "<schema_name>".expected_sessions es
    WHERE es.scheduled_date >= (current_date - (interval '1 month' * <student_window_months>))
),
members AS (
    SELECT gs.organisation_id, grp.uuid AS group_subject_uuid, gm.uuid AS student_subject_uuid,
           gs.membership_start_date, gs.membership_end_date
    FROM public.group_subject gs
    JOIN public.individual grp ON grp.id = gs.group_subject_id AND grp.is_voided = false
    JOIN public.individual gm ON gm.id = gs.member_subject_id AND gm.is_voided = false
    WHERE gs.is_voided = false
),
student_expected AS (
    SELECT we.organisation_id, m.student_subject_uuid, we.group_subject_uuid,
           we.scheduled_date, we.attendance_type_uuid, we.session_status, we.calendar_day_type
    FROM windowed_expected we
    JOIN members m ON m.group_subject_uuid = we.group_subject_uuid
                  AND we.scheduled_date >= COALESCE(m.membership_start_date::date, DATE '-infinity')
                  AND we.scheduled_date \<= COALESCE(m.membership_end_date::date, DATE 'infinity')
),
sess AS (
    SELECT s.uuid AS session_uuid, s.group_subject_uuid, s.scheduled_date, s.attendance_type_uuid
    FROM "<schema_name>".session s
    WHERE s.is_voided = false
),
recs AS (
    SELECT ar.subject_uuid, ar.session_uuid, ar.status, ar.reason_concept_uuids,
           ar.other_reason_text, ar.needs_follow_up, ar.follow_up_encounter_uuid
    FROM "<schema_name>".attendance_record ar
    WHERE ar.is_voided = false
)
SELECT se.organisation_id, se.student_subject_uuid, se.group_subject_uuid,
       se.scheduled_date, se.attendance_type_uuid, se.calendar_day_type,
       CASE
           WHEN r.status = 'Present' THEN 'Present'
           WHEN r.status = 'Absent' THEN 'Absent'
           WHEN r.subject_uuid IS NULL AND se.session_status = 'Held' THEN 'Absent_implied'
           ELSE r.status
       END AS status,
       r.reason_concept_uuids, r.other_reason_text, r.needs_follow_up, r.follow_up_encounter_uuid
FROM student_expected se
JOIN sess s ON s.group_subject_uuid = se.group_subject_uuid
           AND s.scheduled_date = se.scheduled_date
           AND s.attendance_type_uuid = se.attendance_type_uuid
LEFT JOIN recs r ON r.session_uuid = s.session_uuid AND r.subject_uuid = se.student_subject_uuid
WHERE NOT (r.subject_uuid IS NULL AND se.session_status != 'Held');
CREATE INDEX IF NOT EXISTS <view_name>_student_idx ON "<schema_name>"."<view_name>" (organisation_id, student_subject_uuid, scheduled_date);
CREATE INDEX IF NOT EXISTS <view_name>_group_idx ON "<schema_name>"."<view_name>" (organisation_id, group_subject_uuid, scheduled_date);
