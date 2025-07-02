DROP VIEW IF EXISTS "<schema_name>"."<view_name>";
CREATE OR REPLACE VIEW "<schema_name>"."<view_name>" AS
<if(table_metadata)>
    WITH visits AS (
    <table_metadata:{table |
        SELECT
        id,
        uuid,
        name,
        individual_id,
        '<table.encounterTypeUuid>' as encounter_type_uuid,
        earliest_visit_date_time,
        max_visit_date_time,
        created_date_time,
        last_modified_date_time,
        address_id
    from "<schema_name>"."<table.name>"
    <where_clause>
    }; separator="UNION ALL \n">
    )
    SELECT
        visits.*,
        ind.uuid AS individual_uuid,
        ind.first_name,
        ind.last_name,
        et.name as encounter_type_name,
        <address_columns:{col | a."<col>"}; separator=", ">
    FROM visits
    JOIN public.individual ind ON visits.individual_id = ind.id
    JOIN public.encounter_type et ON visits.encounter_type_uuid = et.uuid
    JOIN "<schema_name>".address a ON visits.address_id = a.id;
<else>
    SELECT -1 as id;
<endif>
