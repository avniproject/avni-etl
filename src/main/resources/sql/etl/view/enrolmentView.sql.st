drop view if exists "<schema_name>".enrolment_view;
CREATE OR REPLACE VIEW "<schema_name>".enrolment_view AS
<if(table_metadata)>
    WITH all_enrolments AS (
    <table_metadata:{table |
    SELECT
        id AS enrolment_id,
        uuid AS enrolment_uuid,
        '<table.programUuid>' AS program_uuid,
        individual_id,
        enrolment_date_time,
        created_date_time,
        last_modified_date_time,
        is_voided,
        program_exit_date_time,
        address_id,
        created_by_username,
        last_modified_by_username
    from "<schema_name>"."<table.name>"
    where is_voided is false
    }; separator="UNION ALL \n">
    )
    SELECT all_enrolments.*,
        p.name AS program_name,
        ind.uuid AS individual_uuid,
        ind.first_name,
        ind.last_name,
         <address_columns:{col | a."<col>"}; separator=", ">
    FROM all_enrolments
    JOIN public.program p ON all_enrolments.program_uuid = p.uuid <where_clause>
    JOIN "<schema_name>".address a ON all_enrolments.address_id = a.id
    JOIN public.individual ind ON all_enrolments.individual_id = ind.id;
<else>
    SELECT
        null as enrolment_id,
        null as enrolment_uuid,
        null as program_uuid,
        null as individual_id,
        null as enrolment_date_time,
        null as created_date_time,
        null as last_modified_date_time,
        null as is_voided,
        null as program_exit_date_time,
        null as address_id,
        null as created_by_username,
        null as last_modified_by_username,
        null as program_name,
        null as individual_uuid,
        null as first_name,
        null as last_name,
        <address_columns:{col | null as "<col>"}; separator=", ">
    LIMIT 0;
<endif>
