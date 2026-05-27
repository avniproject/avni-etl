DROP MATERIALIZED VIEW IF EXISTS "<schema_name>"."<view_name>" CASCADE;
CREATE MATERIALIZED VIEW "<schema_name>"."<view_name>" AS
WITH cal AS (
    SELECT c.organisation_id, c.uuid AS calendar_uuid, c.name AS calendar_name, c.working_pattern
    FROM "<schema_name>".calendar c
    WHERE c.is_voided = false
),
dates AS (
    SELECT d::date AS date
    FROM generate_series(DATE '2020-01-01',
                         (date_trunc('year', current_date) + interval '2 year')::date,
                         interval '1 day') d
),
grid AS (
    SELECT cal.organisation_id, cal.calendar_uuid, cal.calendar_name, cal.working_pattern, dates.date,
           CASE extract(isodow from dates.date)
               WHEN 1 THEN 'mon' WHEN 2 THEN 'tue' WHEN 3 THEN 'wed' WHEN 4 THEN 'thu'
               WHEN 5 THEN 'fri' WHEN 6 THEN 'sat' ELSE 'sun' END AS dow_key,
           ((extract(day from dates.date)::int - 1) / 7) + 1 AS occurrence
    FROM cal CROSS JOIN dates
),
marked AS (
    SELECT g.organisation_id, g.calendar_uuid, g.calendar_name, g.date, g.working_pattern, g.dow_key, g.occurrence,
           m.is_working AS marker_is_working, m.name AS marker_name
    FROM grid g
    LEFT JOIN "<schema_name>".calendar_date_marker m
           ON m.calendar_uuid = g.calendar_uuid
          AND m.marker_date = g.date
          AND m.is_voided = false
)
SELECT organisation_id, calendar_uuid, calendar_name, date,
       CASE
           WHEN marker_is_working = false THEN 'public_holiday'
           WHEN marker_is_working = true THEN 'working_override'
           WHEN (working_pattern -> dow_key) = '"all"'::jsonb THEN 'working_day'
           WHEN jsonb_typeof(working_pattern -> dow_key) = 'array'
                AND (working_pattern -> dow_key) @> to_jsonb(occurrence) THEN 'working_day'
           ELSE 'weekly_off'
       END AS day_type,
       CASE WHEN marker_is_working IS NOT NULL THEN marker_name ELSE NULL END AS marker_name
FROM marked;
CREATE INDEX IF NOT EXISTS <view_name>_org_cal_date_idx ON "<schema_name>"."<view_name>" (organisation_id, calendar_uuid, date);
