### Description
The ETL service is responsible for creation, maintenance and distribution of ETL schemas for the Avni database. 

Because of RLS, heavy usage of jsonb columns and hierarchical nature of the address tables, the public schema is not especially suited for analytical queries. This service converts this data structure into a more flat structure with all jsonb keys converted to columns. A scheduled job runs to keep populating data updated from the last run. 

```Organisation and organisation_group``` tables have fields ```schema_name``` that are used by ETL to figure out if an org has ETL enabled, and the schema where data should be transferred to. 
Since form fields can change, ETL adjusts tables to match the new application structure. The schema is stored in the ```table_metadata, column_metadata and index_metadata ``` tables on the public schema. 
```public.entity_sync_status``` stores the current sync status of each table.

### Attendance & Calendars

For orgs using the attendance feature, ETL projects five **passthrough tables** into each org
schema (all rows incl. voided, FK ids resolved to UUIDs): `calendar`, `calendar_date_marker`,
`attendance_type`, `session`, `attendance_record`.

It also builds four **materialized views**, refreshed at the end of each ETL run (dropped and
recreated), only for schemas that have at least one non-voided `calendar`:

- `working_day_calendar(organisation_id, calendar_uuid, calendar_name, date, day_type, marker_name)`
  — one row per (calendar, date) over a rolling window (2020-01-01 .. current year + 2).
- `subject_resolved_calendar(organisation_id, subject_uuid, calendar_uuid)` — the calendar each
  subject resolves to (closest address-level calendar up the hierarchy, else the org default).
- `expected_sessions(organisation_id, group_subject_uuid, group_subject_type_uuid,
  attendance_type_uuid, attendance_type_name, scheduled_date, day_of_week, calendar_day_type, status)`
  — one row per (group, date, attendance_type) on working days, plus "mark-anyway" sessions on
  off days flagged `calendar_day_type = 'mark_anyway'`.
- `per_student_attendance(organisation_id, student_subject_uuid, group_subject_uuid, scheduled_date,
  attendance_type_uuid, status, reason_concept_uuid, follow_up_encounter_uuid)` — rolling window of
  the last `AVNI_ATTENDANCE_PER_STUDENT_WINDOW_IN_MONTHS` months (default 12).

**`day_type` terminology:** `weekly_off` = the calendar's `working_pattern` says non-working;
`public_holiday` = a marker says non-working; `working_override` = a marker says working despite the
pattern. `marker_name` is populated only for `public_holiday` and `working_override`.

**Example queries**

```sql
-- Attendance % per group (working/override days only; excludes mark-anyway)
SELECT group_subject_uuid,
       round(100.0 * count(*) FILTER (WHERE status = 'Present') / nullif(count(*), 0), 1) AS attendance_pct
FROM per_student_attendance
GROUP BY group_subject_uuid;

-- Holiday impact: working days lost to public holidays per calendar
SELECT calendar_name, count(*) AS public_holidays
FROM working_day_calendar
WHERE day_type = 'public_holiday'
GROUP BY calendar_name;

-- Marker-name lookup for a date
SELECT calendar_name, date, day_type, marker_name
FROM working_day_calendar
WHERE day_type IN ('public_holiday', 'working_override');

-- attendance_type.config filters (JSON ops)
SELECT * FROM attendance_type WHERE config->>'follow_up_encounter_type_uuid' = '<uuid>';
SELECT * FROM attendance_type WHERE (config->>'auto_share_on_save')::boolean = true;
```

### Developer setup
- Use jenv to match java version (Add whatever java version is mentioned)
- Clone avni-server and run ```make build_db build_test_db``` to have all required tables in your system
- ```make start``` to start server
- ```make test``` to run tests


[![CircleCI](https://circleci.com/gh/avniproject/avni-etl/tree/main.svg?style=svg)](https://circleci.com/gh/avniproject/avni-etl/tree/main)
