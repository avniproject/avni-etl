CREATE OR REPLACE VIEW <schema_name>.<schema_name>_<postfix> AS
SELECT
    ind.id AS individual_id,
    st.name AS subject_type,
    ind.uuid AS individual_uuid,
    ind.first_name,
    ind.last_name,
    ind.middle_name,
    ind.date_of_birth,
    g.name AS gender,
    ind.registration_date AS individual_registration_date,
    ind.created_date_time AS individual_created_date_time,
    ind.last_modified_date_time As individual_last_modified_date_time,
    address.*
FROM public.individual ind
         JOIN public.subject_type st ON ind.subject_type_id = st.id
         JOIN <schema_name>.address ON ind.address_id = address.id
    LEFT JOIN public.gender g ON ind.gender_id = g.id;