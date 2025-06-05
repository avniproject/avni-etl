DROP VIEW IF EXISTS "<schema_name>".subject_view;
<if(table_metadata)>
CREATE OR REPLACE VIEW "<schema_name>".subject_view AS
WITH all_subjects AS (
<table_metadata:{table |
SELECT
    id as individual_id,
    uuid as indvidual_uuid,
    first_name,
<if(table.isPerson)> date_of_birth <else> null <endif> as date_of_birth,
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
 <endif>
