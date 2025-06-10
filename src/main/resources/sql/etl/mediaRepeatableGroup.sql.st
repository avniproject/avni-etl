insert into <schemaName>.<tableName> (
    -- Entity identifiers
    entity_id, uuid, is_voided, created_by_id, last_modified_by_id, 
    created_date_time, last_modified_date_time, organisation_id, address_id, 
    -- Media and sync data
    image_url, sync_parameter_key1, sync_parameter_value1, sync_parameter_key2, sync_parameter_value2, 
    -- Subject and concept data
    subject_type_name, encounter_type_name, program_name, concept_name, 
    subject_first_name, subject_last_name, subject_middle_name, 
    -- Media metadata and form data
    media_metadata, form_element_id, repeatable_question_group_index)
select 
    -- Basic entity data
    entity.id,
    entity.id as uuid, -- Use ID as fallback since uuid column doesn't exist
    COALESCE(entity.is_voided, parent.is_voided, false) as is_voided, -- Use parent value or default
    parent.created_by_id,  -- Get from parent
    parent.last_modified_by_id, -- Get from parent
    COALESCE(entity.created_date_time, parent.created_date_time) as created_date_time,
    COALESCE(entity.last_modified_date_time, parent.last_modified_date_time) as last_modified_date_time,
    COALESCE(entity.organisation_id, parent.organisation_id) as organisation_id, -- Get from entity or parent
    COALESCE(entity.address_id, parent.address_id) as address_id, -- Get from entity or parent
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
    subject.middle_name,
    -- Media metadata as JSON
    json_build_object('uri', media_element)::jsonb as media_metadata,
    -- Form element ID with fallback
    COALESCE(
        CASE WHEN entity."<conceptColumnName>" LIKE '[%' THEN 
            json_array_elements_text(entity."<conceptColumnName>"::json)::json->>'formElementId'
        ELSE NULL END,
        '<formElementUuid>'
    ) as form_element_id,
    -- Repeatable group index with fallback to row_number
    CASE
        WHEN entity."index" IS NOT NULL THEN entity."index"
        ELSE (ROW_NUMBER() OVER (PARTITION BY entity."<subjectIdColumnName>" ORDER BY entity.created_date_time) - 1)::integer
    END as repeatable_question_group_index
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
        SELECT json_array_elements_text(entity."<conceptColumnName>"::json) as media_element
        WHERE entity."<conceptColumnName>" IS NOT NULL AND entity."<conceptColumnName>" LIKE '[%'
        
        UNION ALL
        
        -- Direct URL case
        SELECT entity."<conceptColumnName>" as media_element
        WHERE entity."<conceptColumnName>" IS NOT NULL 
          AND entity."<conceptColumnName>" NOT LIKE '[%' 
          AND entity."<conceptColumnName>" LIKE 'http%'
    ) as expanded_media
where 
    entity.last_modified_date_time > '<startTime>'
    and entity.last_modified_date_time \<= '<endTime>'
    and entity."<conceptColumnName>" is not null;
