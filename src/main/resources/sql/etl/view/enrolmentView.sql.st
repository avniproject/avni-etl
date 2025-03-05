drop view if exists <schema_name>.<view_name>;
CREATE OR REPLACE VIEW <schema_name>.<view_name> AS
SELECT
    pe.id AS enrolment_id,
    pe.uuid AS enrolment_uuid,
    p.name AS program_name,
    ind.id AS individual_id,
    ind.uuid AS individual_uuid,
    ind.first_name,
    ind.last_name,
    pe.enrolment_date_time,
    pe.created_date_time AS created_date_time,
    pe.last_modified_date_time AS last_modified_date_time,
    pe.is_voided,
    pe.program_exit_date_time AS program_exit_date_time,
    <address_columns>
FROM public.program_enrolment pe
JOIN public.individual ind ON pe.individual_id = ind.id and ind.is_voided=false
JOIN public.program p ON pe.program_id = p.id and p.is_voided=false
JOIN <schema_name>.address ON ind.address_id = address.id;
