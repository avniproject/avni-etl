DROP MATERIALIZED VIEW IF EXISTS "<schema_name>"."<view_name>" CASCADE;
CREATE MATERIALIZED VIEW "<schema_name>"."<view_name>" AS
WITH subjects AS (
    SELECT i.organisation_id, i.uuid AS subject_uuid, i.address_id
    FROM public.individual i
    WHERE i.is_voided = false
),
subj_addr AS (
    SELECT s.organisation_id, s.subject_uuid, al.lineage
    FROM subjects s
    JOIN public.address_level al ON al.id = s.address_id
),
ancestors AS (
    SELECT sa.organisation_id, sa.subject_uuid, anc.uuid AS address_level_uuid,
           nlevel(anc.lineage) AS depth
    FROM subj_addr sa
    JOIN public.address_level anc ON anc.lineage @> sa.lineage
),
attached AS (
    SELECT a.organisation_id, a.subject_uuid, c.uuid AS calendar_uuid,
           row_number() OVER (PARTITION BY a.subject_uuid ORDER BY a.depth DESC) AS rn
    FROM ancestors a
    JOIN "<schema_name>".calendar c ON c.address_level_uuid = a.address_level_uuid AND c.is_voided = false
),
hierarchy_pick AS (
    SELECT organisation_id, subject_uuid, calendar_uuid FROM attached WHERE rn = 1
),
org_default AS (
    SELECT c.organisation_id, c.uuid AS calendar_uuid
    FROM "<schema_name>".calendar c
    WHERE c.is_voided = false AND c.is_default = true AND c.address_level_uuid IS NULL
)
SELECT s.organisation_id, s.subject_uuid,
       COALESCE(hp.calendar_uuid, od.calendar_uuid) AS calendar_uuid
FROM subjects s
LEFT JOIN hierarchy_pick hp ON hp.subject_uuid = s.subject_uuid
LEFT JOIN org_default od ON od.organisation_id = s.organisation_id
WHERE COALESCE(hp.calendar_uuid, od.calendar_uuid) IS NOT NULL;
CREATE INDEX IF NOT EXISTS <view_name>_subj_idx ON "<schema_name>"."<view_name>" (organisation_id, subject_uuid);
CREATE INDEX IF NOT EXISTS <view_name>_cal_idx ON "<schema_name>"."<view_name>" (organisation_id, calendar_uuid);
