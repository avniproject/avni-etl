DROP FUNCTION IF EXISTS "<schema_name>".subject_view_function() CASCADE;

CREATE OR REPLACE FUNCTION "<schema_name>".subject_view_function()
RETURNS TABLE (
    individual_id int,
    subject_type varchar,
    individual_uuid varchar(255),
    first_name varchar,
    last_name varchar,
    middle_name varchar,
    date_of_birth date,
    gender varchar,
    registration_date date,
    created_date_time timestamptz,
    last_modified_date_time timestamptz,
    is_voided boolean,
     <address_columns:{col | "<col>" text}; separator=", ">
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        ind.id AS individual_id,
        st.name AS subject_type,
        ind.uuid AS individual_uuid,
        ind.first_name,
        ind.last_name,
        ind.middle_name,
        ind.date_of_birth,
        g.name AS gender,
        ind.registration_date AS registration_date,
        ind.created_date_time AS created_date_time,
        ind.last_modified_date_time As last_modified_date_time,
        ind.is_voided,
         <address_columns:{col | a."<col>"}; separator=", ">
    FROM public.individual ind
        JOIN public.subject_type st ON ind.subject_type_id = st.id and st.is_voided=false
        JOIN "<schema_name>".address a ON ind.address_id = a.id
        LEFT JOIN public.gender g ON ind.gender_id = g.id
    <where_clause>;
END;
$$ LANGUAGE plpgsql;

DROP VIEW IF EXISTS "<schema_name>".subject_view;

CREATE OR REPLACE VIEW "<schema_name>".subject_view AS
SELECT * FROM "<schema_name>".subject_view_function();