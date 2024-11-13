SELECT *, row_to_json(address.*) as address FROM
(
         SELECT media.id as id,
                media.uuid as uuid,
                media.subject_first_name as subject_first_name,
                media.subject_last_name as subject_last_name,
                media.subject_middle_name as subject_middle_name,
                media.is_voided as is_voided,
                media.created_by_id as created_by_id,
                media.last_modified_by_id as last_modified_by_id,
                media.created_date_time as created_date_time,
                media.last_modified_date_time as last_modified_date_time,
                media.organisation_id as organisation_id,
                media.image_url as image_url,
                media.sync_parameter_key1 as sync_parameter_key1,
                media.sync_parameter_key2 as sync_parameter_key2,
                media.sync_parameter_value1 as sync_parameter_value1,
                media.sync_parameter_value2 as sync_parameter_value2,
                media.subject_type_name as subject_type_name,
                media.encounter_type_name as encounter_type_name,
                media.program_name as program_name,
                media.concept_name as concept_name,
                media.address_id as address_id,
                media.entity_id as entity_id
         FROM <schemaName>.media media
                  <if(joinTablesAndColumns)>
                    <joinTablesAndColumns:{joinTableAndColumn |
                    RIGHT JOIN <schemaName>.<joinTableAndColumn.tableName> <joinTableAndColumn.tableName> ON media.entity_id = <joinTableAndColumn.tableName>.id
                    <if(joinTableAndColumn.exactSearch)>
                      AND <joinTableAndColumn.tableName>."<joinTableAndColumn.columnName>"
                        <if(joinTableAndColumn.columnValues)>
                          IN ('<joinTableAndColumn.columnValues:{columnValue | <columnValue>}; separator="', '">')
                        <else>
                          <! BETWEEN <if(joinTableAndColumn.nonStringValue)>if <joinTableAndColumn.from> AND <joinTableAndColumn.to><else> else '<joinTableAndColumn.from>' AND '<joinTableAndColumn.to>'<endif> !>
                          <! Below works because postgresql does an implicit type cast to numeric on passing strings for comparing with numeric. Revert to above approach after fixing (currently nonStringValue is always false despite setting boolean) if this causes issues later. !>
                          BETWEEN '<joinTableAndColumn.from>' AND '<joinTableAndColumn.to>'
                        <endif>
                    <else>
                      AND <joinTableAndColumn.columnValues:{columnValue | <joinTableAndColumn.tableName>."<joinTableAndColumn.columnName>" ILIKE '%<columnValue>%'}; separator=" \nAND ">
                    <endif>
                   }; separator="\n">
                  <endif>
         where media.image_url is not null
             and media.is_voided is false
             <if(request)>
             <if(request.fromDate)>                  and media.created_date_time >= :fromDate                   <endif>
             <if(request.toDate)>                    and media.created_date_time \<= :toDate                     <endif>
             <if(request.subjectName)>               and  (<request.subjectNameTokens : {subjectNameToken |
                             (media.subject_first_name ilike '%<subjectNameToken>%' or media.subject_middle_name ilike '%<subjectNameToken>%' or media.subject_last_name ilike '%<subjectNameToken>%')
                             }; separator="\n AND ">)
             <endif>
             <if(request.subjectTypeNames)>          and media.subject_type_name in (:subjectTypeNames)          <endif>
             <if(request.programNames)>              and media.program_name in (:programNames)                   <endif>
             <if(request.encounterTypeNames)>        and media.encounter_type_name in (:encounterTypeNames)      <endif>
             <if(request.imageConcepts)>             and media.concept_name in (:imageConcepts)                  <endif>
             <if(request.syncValues)> and (<endif>
               <request.syncValues:{syncValue|
                 ((media.sync_parameter_key1 = :syncConceptName_<i0> and media.sync_parameter_value1 in (:syncConceptValues_<i0>))
                 or (media.sync_parameter_key2 = :syncConceptName_<i0> and media.sync_parameter_value2 in (:syncConceptValues_<i0>)))};
                 separator="\n OR "
               >
             <if(request.syncValues)> )<endif>
             <if(request.addresses)> and (<endif>
                 <request.addresses:{addressRequest|<if(addressRequest.addressLevelIds)>address."<addressRequest.addressLevelType> id" in (:addressLevelIds_<i0>)<else>1 = 2<endif>}; separator="\n OR ">
             <if(request.addresses)> )<endif>
             <endif>
         ORDER BY media.created_date_time desc
         LIMIT :limit OFFSET :offset
) media_search_result
         JOIN <schemaName>.address address ON address.id = media_search_result.address_id
                  ORDER BY media_search_result.created_date_time desc
;
