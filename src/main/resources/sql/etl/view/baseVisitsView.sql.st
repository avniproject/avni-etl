DROP FUNCTION IF EXISTS "<schema_name>".<view_name>_function() CASCADE;

CREATE OR REPLACE FUNCTION "<schema_name>".<view_name>_function()
RETURNS TABLE (
    id int,
    uuid varchar(255),
    encounter_type_name varchar,
    individual_uuid varchar(255),
    first_name varchar,
    last_name varchar,
    earliest_visit_date_time timestamptz,
    encounter_date_time timestamptz,
    max_visit_date_time timestamptz,
    created_date_time timestamptz,
    last_modified_date_time timestamptz,
     <address_columns:{col | "<col>" text}; separator=", ">
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.id AS id,
        e.uuid AS uuid,
        et.name AS encounter_type_name,
        ind.uuid AS individual_uuid,
        ind.first_name,
        ind.last_name,
        e.earliest_visit_date_time,
        e.encounter_date_time,
        e.max_visit_date_time,
        e.created_date_time,
        e.last_modified_date_time,
         <address_columns:{col | a."<col>"}; separator=", ">
    FROM encounter e
    JOIN public.individual ind ON e.individual_id = ind.id
    JOIN public.encounter_type et ON e.encounter_type_id = et.id
    JOIN "<schema_name>".address a ON ind.address_id = a.id
    <where_clause>;
END;
$$ LANGUAGE plpgsql;

DROP VIEW IF EXISTS "<schema_name>"."<view_name>";

CREATE OR REPLACE VIEW "<schema_name>"."<view_name>" AS
SELECT * FROM "<schema_name>".<view_name>_function();
