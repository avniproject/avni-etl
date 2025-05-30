CREATE OR REPLACE VIEW "<schema_name>"."<view_name>" AS
SELECT
    e.id AS id,
    e.uuid AS uuid,
    et.name AS encounter_type_name,
    ind.uuid AS individual_uuid,
    ind.first_name,
    ind.last_name,
    e.earliest_visit_date_time,
    <extra_columns>
    e.max_visit_date_time,
    e.created_date_time,
    e.last_modified_date_time,
    <address_columns>
FROM encounter e
JOIN public.individual ind ON e.individual_id = ind.id
JOIN public.encounter_type et ON e.encounter_type_id = et.id
JOIN "<schema_name>".address ON ind.address_id = address.id
<where_clause>