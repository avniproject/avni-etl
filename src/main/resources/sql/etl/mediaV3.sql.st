insert into <schemaName>.<tableName> (
    entity_id, uuid, is_voided, created_by_id, last_modified_by_id, 
    created_date_time, last_modified_date_time, organisation_id, address_id, 
    image_url, sync_parameter_key1, sync_parameter_value1, sync_parameter_key2, sync_parameter_value2, 
    subject_type_name, encounter_type_name, program_name, concept_name, 
    subject_first_name, subject_last_name, subject_middle_name, media_metadata, 
    form_element_id, repeatable_question_group_index)
select 
    entity.id,
    entity.id as uuid, -- Use ID as fallback since uuid column doesn't exist
    COALESCE(entity.is_voided, subject.is_voided, false) as is_voided, -- Use parent value or default
    COALESCE(subject.created_by_id, null) as created_by_id, -- Get from parent
    COALESCE(subject.last_modified_by_id, null) as last_modified_by_id, -- Get from parent
    COALESCE(entity.created_date_time, subject.created_date_time) as created_date_time,
    COALESCE(entity.last_modified_date_time, subject.last_modified_date_time) as last_modified_date_time,
    COALESCE(entity.organisation_id, subject.organisation_id) as organisation_id, -- Get from entity or parent
    COALESCE(entity.address_id, subject.address_id) as address_id, -- Get from entity or parent
    -- Extract URI from JSON element
    media_json::json->>'uri' as image_url,
    -- Sync parameters
    <if(syncRegistrationConcept1ColumnName)><syncRegistrationConcept1Name>, entity."<syncRegistrationConcept1ColumnName>",<else>null, null,<endif>
    <if(syncRegistrationConcept2ColumnName)><syncRegistrationConcept2Name>, entity."<syncRegistrationConcept2ColumnName>",<else>null, null,<endif>
    -- Entity metadata
    <subjectTypeName>,
    <encounterTypeName>,
    <programName>,
    <conceptName>,
    subject.first_name,
    subject.last_name,
    <if(hasMiddleName)>subject.middle_name<else>null<endif>,
    -- Media data
    media_json::jsonb as media_metadata,
    -- Form element ID 
    COALESCE(media_json::json->>'formElementId', '<conceptColumnName>') as form_element_id,
    -- Question group index
    NULLIF((media_json::json->>'repeatableQuestionGroupIndex')::integer, -1) as repeatable_question_group_index
from 
    <schemaName>.<fromTableName> entity
    -- Join with the subject table to get missing columns
    inner join <schemaName>.<subjectTableName> subject 
        on entity."<subjectIdColumnName>" = subject.id,
    json_array_elements_text(entity."<conceptColumnName>"::json) as media_json
where 
    entity."last_modified_date_time" > '<startTime>'
    and entity."last_modified_date_time" \<= '<endTime>'
    and entity."<conceptColumnName>" is not null;
