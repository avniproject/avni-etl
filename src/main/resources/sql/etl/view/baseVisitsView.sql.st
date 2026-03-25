DROP VIEW IF EXISTS "<schema_name>"."<view_name>";
CREATE OR REPLACE VIEW "<schema_name>"."<view_name>" AS
<if(table_metadata)>
    WITH visits AS (
    <table_metadata:{table |
        SELECT
        t.id,
        t.uuid,
        t.name,
        t.individual_id,
        '<table.encounterTypeUuid>' as encounter_type_uuid,
        t.encounter_date_time,
        t.cancel_date_time,
        t.is_voided,
        t.earliest_visit_date_time,
        t.max_visit_date_time,
        t.created_date_time,
        t.last_modified_date_time,
        t.address_id,
        created_user.username as created_by_username,
        modified_user.username as last_modified_by_username,
        filled_user.username as filled_by_username
    from "<schema_name>"."<table.name>" t
    LEFT JOIN public.users created_user on t.created_by_id = created_user.id
    LEFT JOIN public.users modified_user on t.last_modified_by_id = modified_user.id
    LEFT JOIN public.users filled_user on t.filled_by_id = filled_user.id
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
    SELECT
        null as id,
        null as uuid,
        null as name,
        null as individual_id,
        null as encounter_type_uuid,
        null as encounter_date_time,
        null as cancel_date_time,
        null as is_voided,
        null as earliest_visit_date_time,
        null as max_visit_date_time,
        null as created_date_time,
        null as last_modified_date_time,
        null as address_id,
        null as created_by_username,
        null as last_modified_by_username,
        null as filled_by_username,
        null as individual_uuid,
        null as first_name,
        null as last_name,
        null as encounter_type_name,
        <address_columns:{col | null as "<col>"}; separator=", ">
    LIMIT 0;
<endif>
