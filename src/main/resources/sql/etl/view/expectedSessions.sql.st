DROP MATERIALIZED VIEW IF EXISTS "<schema_name>"."<view_name>" CASCADE;
CREATE MATERIALIZED VIEW "<schema_name>"."<view_name>" AS
<if(table_metadata)>
WITH groups AS (
    -- Group subjects read directly from the org-schema per-subject-type tables (Group/Household),
    -- so this view depends only on the ETL schema (no public.* and no subject_view). Each table
    -- maps to a single subject type, so its uuid is a literal. "Attendance enabled" is implied by
    -- the join to the attendance_type passthrough below (the server only allows attendance types on
    -- enabled subject types, so a non-enabled type contributes no rows).
    <table_metadata:{table |
        SELECT t.organisation_id, t.uuid AS group_subject_uuid, '<table.subjectTypeUuid>' AS group_subject_type_uuid
        FROM "<schema_name>"."<table.name>" t
        WHERE t.is_voided = false
    }; separator="UNION ALL ">
),
group_attendance_types AS (
    SELECT gs.organisation_id, gs.group_subject_uuid, gs.group_subject_type_uuid,
           at.uuid AS attendance_type_uuid, at.name AS attendance_type_name
    FROM groups gs
    JOIN "<schema_name>".attendance_type at
         ON at.subject_type_uuid = gs.group_subject_type_uuid AND at.is_voided = false
),
resolved AS (
    SELECT gat.organisation_id, gat.group_subject_uuid, gat.group_subject_type_uuid,
           gat.attendance_type_uuid, gat.attendance_type_name, src.calendar_uuid
    FROM group_attendance_types gat
    JOIN "<schema_name>".subject_resolved_calendar src ON src.subject_uuid = gat.group_subject_uuid
),
calendar_days AS (
    SELECT r.organisation_id, r.group_subject_uuid, r.group_subject_type_uuid,
           r.attendance_type_uuid, r.attendance_type_name,
           wdc.date AS scheduled_date, wdc.day_type AS calendar_day_type
    FROM resolved r
    JOIN "<schema_name>".working_day_calendar wdc
         ON wdc.calendar_uuid = r.calendar_uuid AND wdc.day_type IN ('working_day', 'working_override')
),
sess AS (
    SELECT s.group_subject_uuid, s.attendance_type_uuid, s.scheduled_date, s.status
    FROM "<schema_name>".session s
    WHERE s.is_voided = false
),
expected AS (
    SELECT cd.organisation_id, cd.group_subject_uuid, cd.group_subject_type_uuid,
           cd.attendance_type_uuid, cd.attendance_type_name, cd.scheduled_date,
           CASE extract(isodow from cd.scheduled_date)
               WHEN 1 THEN 'mon' WHEN 2 THEN 'tue' WHEN 3 THEN 'wed' WHEN 4 THEN 'thu'
               WHEN 5 THEN 'fri' WHEN 6 THEN 'sat' ELSE 'sun' END AS day_of_week,
           cd.calendar_day_type,
           COALESCE(se.status, 'Unmarked') AS status
    FROM calendar_days cd
    LEFT JOIN sess se ON se.group_subject_uuid = cd.group_subject_uuid
                      AND se.attendance_type_uuid = cd.attendance_type_uuid
                      AND se.scheduled_date = cd.scheduled_date
),
mark_anyway AS (
    -- Sessions recorded on calendar-off (or out-of-window) dates, appended and flagged.
    -- Join through `resolved` (not the raw session/attendance_type) so this branch applies the
    -- same filters as `expected`: the group is attendance-enabled and non-voided, the attendance
    -- type is configured for its subject type and non-voided, and the group's calendar resolves
    -- (unresolved groups are excluded, matching `expected`'s inner join).
    SELECT r.organisation_id, r.group_subject_uuid, r.group_subject_type_uuid,
           r.attendance_type_uuid, r.attendance_type_name, se.scheduled_date,
           CASE extract(isodow from se.scheduled_date)
               WHEN 1 THEN 'mon' WHEN 2 THEN 'tue' WHEN 3 THEN 'wed' WHEN 4 THEN 'thu'
               WHEN 5 THEN 'fri' WHEN 6 THEN 'sat' ELSE 'sun' END AS day_of_week,
           'mark_anyway' AS calendar_day_type,
           COALESCE(se.status, 'Unmarked') AS status
    FROM sess se
    JOIN resolved r ON r.group_subject_uuid = se.group_subject_uuid
                   AND r.attendance_type_uuid = se.attendance_type_uuid
    LEFT JOIN "<schema_name>".working_day_calendar wdc
           ON wdc.calendar_uuid = r.calendar_uuid AND wdc.date = se.scheduled_date
    WHERE wdc.date IS NULL OR wdc.day_type NOT IN ('working_day', 'working_override')
)
SELECT * FROM expected
UNION ALL
SELECT * FROM mark_anyway
<else>
-- Org has no group/household subject tables: empty view with the right shape.
SELECT null::integer AS organisation_id, null::text AS group_subject_uuid, null::text AS group_subject_type_uuid,
       null::text AS attendance_type_uuid, null::text AS attendance_type_name, null::date AS scheduled_date,
       null::text AS day_of_week, null::text AS calendar_day_type, null::text AS status
LIMIT 0
<endif>;
CREATE INDEX IF NOT EXISTS <view_name>_grp_date_at_idx ON "<schema_name>"."<view_name>" (organisation_id, group_subject_uuid, scheduled_date, attendance_type_uuid);
