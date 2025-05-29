DROP FUNCTION IF EXISTS "<schema_name>".enrolment_view_function() CASCADE;

CREATE OR REPLACE FUNCTION "<schema_name>".enrolment_view_function()
RETURNS TABLE (
    program_enrolment_id int,
    uuid varchar(255),
    individual_id int,
    individual_uuid varchar(255),
    first_name varchar,
    last_name varchar,
    program_name varchar,
    enrolment_date_time timestamptz,
    created_date_time timestamptz,
    last_modified_date_time timestamptz,
    is_voided boolean,
     <address_columns:{col | "<col>" text}; separator=", ">
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        pe.id as program_enrolment_id,
        pe.uuid as uuid,
        ind.id as individual_id,
        ind.uuid as individual_uuid,
        ind.first_name,
        ind.last_name,
        p.name as program_name,
        pe.enrolment_date_time,
        pe.created_date_time,
        pe.last_modified_date_time,
        pe.is_voided,
         <address_columns:{col | a."<col>"}; separator=", ">
    FROM program_enrolment pe
    JOIN public.individual ind ON pe.individual_id = ind.id
    JOIN public.program p ON pe.program_id = p.id
    JOIN "<schema_name>".address a ON ind.address_id = a.id
    <where_clause>;
END;
$$ LANGUAGE plpgsql;

DROP VIEW IF EXISTS "<schema_name>".enrolment_view;

CREATE OR REPLACE VIEW "<schema_name>".enrolment_view AS
SELECT * FROM "<schema_name>".enrolment_view_function();