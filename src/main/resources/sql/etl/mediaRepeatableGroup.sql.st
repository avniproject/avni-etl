insert into <schemaName>.<tableName> (
    -- Entity identifiers
    entity_id, uuid, is_voided, created_by_id, last_modified_by_id, 
    created_date_time, last_modified_date_time, organisation_id, address_id, 
    -- Media and sync data
    image_url, sync_parameter_key1, sync_parameter_value1, sync_parameter_key2, sync_parameter_value2, 
    -- Subject and concept data
    subject_type_name, encounter_type_name, program_name, concept_name, 
    subject_first_name, subject_last_name, 
    -- Media metadata and form data
    media_metadata, question_group_concept_name, repeatable_question_group_index)
select 
    -- Basic entity data
    parent.id as entity_id,
    parent.uuid as uuid, -- Use parent UUID since repeatable groups don't have their own UUID
    COALESCE(entity.is_voided, parent.is_voided, false) as is_voided, -- Use parent value or default
    parent.created_by_id,  -- Get from parent
    parent.last_modified_by_id, -- Get from parent
    COALESCE(parent.created_date_time, parent.created_date_time) as created_date_time,
    COALESCE(parent.last_modified_date_time, parent.last_modified_date_time) as last_modified_date_time,
    COALESCE(parent.organisation_id, parent.organisation_id) as organisation_id, -- Get from entity or parent
    COALESCE(parent.address_id, parent.address_id) as address_id, -- Get from entity or parent
    -- Media URL from lateral join
    media_element as image_url,
    -- Sync parameters with default values (no conditionals)
    null as sync_parameter_key1, 
    null as sync_parameter_value1,
    null as sync_parameter_key2, 
    null as sync_parameter_value2,
    -- Entity and concept metadata
    <subjectTypeName>,
    <encounterTypeName>,
    <programName>,
    <conceptName>,
    -- Subject information
    subject.first_name,
    subject.last_name,
    json_build_object('uri', media_element)::jsonb as media_metadata,
    '<questionGroupConceptName>' as question_group_concept_name,
    (ROW_NUMBER() OVER (PARTITION BY entity."<subjectIdColumnName>" ORDER BY entity.id) - 1)::integer as repeatable_question_group_index
from 
    <schemaName>.<fromTableName> entity
    inner join <schemaName>.<subjectTableName> subject 
        on entity."<subjectIdColumnName>" = subject.id
    -- Join with parent entity to get missing columns
    inner join <schemaName>.<parentTableName> parent
        on entity."<parentIdColumnName>" = parent.id,
    -- Lateral join to handle both JSON arrays and direct URLs
    LATERAL (
        -- JSON array case
        SELECT json_array_elements_text(REPLACE(REPLACE(entity."<conceptColumnName>"::text, '\"', '"'), '"[', '[')::json) as media_element
        WHERE entity."<conceptColumnName>" IS NOT NULL AND entity."<conceptColumnName>"::text LIKE '[%'
        
        UNION ALL
        
        -- Direct URL case
        SELECT entity."<conceptColumnName>"::text as media_element
        WHERE entity."<conceptColumnName>" IS NOT NULL 
          AND entity."<conceptColumnName>"::text NOT LIKE '[%' 
          AND entity."<conceptColumnName>"::text LIKE 'http%'
    ) as expanded_media
where 
    entity.last_modified_date_time > '<startTime>'
    and entity.last_modified_date_time \<= '<endTime>'
    and entity."<conceptColumnName>" is not null;
