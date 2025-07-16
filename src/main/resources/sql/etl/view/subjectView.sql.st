DROP VIEW IF EXISTS "<schema_name>".subject_view;
CREATE OR REPLACE VIEW "<schema_name>".subject_view AS
<if(table_metadata)>
    WITH all_subjects AS (
    <table_metadata:{table |
    SELECT
        id as individual_id,
        uuid as indvidual_uuid,
        first_name,
    <if(table.isPerson)> date_of_birth <else> null::date <endif> as date_of_birth,
        last_name,
    <if(table.isPerson)> gender <else> null <endif> as gender,
        registration_date,
        created_date_time,
        last_modified_date_time,
        is_voided,
        '<table.subjectTypeUuid>' as subject_type_uuid,
        address_id
    from "<schema_name>"."<table.name>"
    where is_voided is false
    }; separator="UNION ALL \n">
    )
    SELECT all_subs.*,
     st.name as subject_type,
     <address_columns:{col | a."<col>"}; separator=", ">
     FROM all_subjects all_subs
      JOIN public.subject_type st on st.uuid = all_subs.subject_type_uuid <where_clause>
      JOIN "<schema_name>".address a ON all_subs.address_id = a.id;
<else>
    SELECT
        null as individual_id,
        null as indvidual_uuid,
        null as first_name,
        null as date_of_birth,
        null as last_name,
        null as gender,
        null as registration_date,
        null as created_date_time,
        null as last_modified_date_time,
        null as is_voided,
        null as subject_type_uuid,
        null as address_id,
        null as subject_type,
        <address_columns:{col | null as "<col>"}; separator=", ">
    LIMIT 0;
<endif>
