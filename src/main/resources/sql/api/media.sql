SELECT media.id as id,
       media.uuid as uuid,
       media.is_voided as is_voided,
       media.created_by_id as created_by_id,
       media.last_modified_by_id as last_modified_by_id,
       media.created_date_time as created_date_time,
       media.last_modified_date_time as last_modified_date_time,
       media.image_url as image_url,
       media.sync_parameter_key1 as sync_parameter_key1,
       media.sync_parameter_key2 as sync_parameter_key2,
       media.sync_parameter_value1 as sync_parameter_value1,
       media.sync_parameter_value2 as sync_parameter_value2,
       media.subject_type_name as subject_type_name,
       media.encounter_type_name as encounter_type_name,
       media.program_name as program_name,
       media.concept_name as concept_name,
       row_to_json(address.*) as address
FROM <schemaName>.media media
         JOIN <schemaName>.address address ON address.id = media.address_id
where media.image_url is not null
    and media.is_voided is false
    <if(request.fromDate & request.toDate)> and media.created_date_time between :fromDate and :toDate   <endif>
    <if(request.subjectTypeNames)>          and media.subject_type_name in (:subjectTypeNames)          <endif>
    <if(request.programNames)>              and media.program_name in (:programNames)                   <endif>
    <if(request.encounterTypeNames)>        and media.encounter_type_name in (:encounterTypeNames)      <endif>
    <if(request.imageConcepts)>             and media.concept_name in (:imageConcepts)                  <endif>
ORDER BY media.created_date_time desc
LIMIT :limit OFFSET :offset;
<abc.x>
