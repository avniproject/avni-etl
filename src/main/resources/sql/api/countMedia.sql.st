SELECT count(*)
FROM "<schemaName>".media media
         <if(request.addresses)>JOIN "<schemaName>".address address ON address.id = media.address_id<endif>
         <if(joinTablesAndColumns)>
           <joinTablesAndColumns:{joinTableAndColumn |
           RIGHT JOIN "<schemaName>".<joinTableAndColumn.tableName> <joinTableAndColumn.tableName>_<i0> ON media.entity_id = <joinTableAndColumn.tableName>_<i0>.id
           <if(joinTableAndColumn.exactSearch)>
             AND <joinTableAndColumn.tableName>_<i0>."<joinTableAndColumn.columnName>"
               <if(joinTableAndColumn.columnValues)>
                 IN ('<joinTableAndColumn.columnValues:{columnValue | <columnValue>}; separator="', '">')
               <else>
                 <! BETWEEN <if(joinTableAndColumn.nonStringValue)>if <joinTableAndColumn.from> AND <joinTableAndColumn.to><else> else '<joinTableAndColumn.from>' AND '<joinTableAndColumn.to>'<endif> !>
                 <! Below works because postgresql does an implicit type cast to numeric on passing strings for comparing with numeric. Revert to above approach after fixing (currently nonStringValue is always false despite setting boolean) if this causes issues later. !>
                 BETWEEN '<joinTableAndColumn.from>' AND '<joinTableAndColumn.to>'
               <endif>
           <else>
             AND <joinTableAndColumn.columnValues:{columnValue | <joinTableAndColumn.tableName>_<i0>."<joinTableAndColumn.columnName>" ILIKE '%<columnValue>%'}; separator=" \nAND ">
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
;
