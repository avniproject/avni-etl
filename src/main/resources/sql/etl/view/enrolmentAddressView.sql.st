CREATE OR REPLACE VIEW "<schema_name>".<view_name> AS
SELECT
    pe.id AS enrolment_id,
    pe.uuid AS enrolment_uuid,
    p.name AS program_name,
    ind.id AS individual_id,
    ind.uuid AS individual_uuid,
    ind.first_name,
    ind.last_name,
    pe.enrolment_date_time,
    pe.created_date_time AS enrolment_created_date_time,
    pe.last_modified_date_time AS enrolment_last_modified_date_time
FROM public.individual ind
JOIN public.program_enrolment pe ON ind.id = pe.individual_id
JOIN public.program p ON pe.program_id = p.id
JOIN "<schema_name>".address ON ind.address_id = address.id;
