select fm.uuid                                                                   form_mapping_uuid,
       f.uuid                                                                    form_uuid,
       ost.name                                                                  subject_type_name,
       st.uuid                                                                   subject_type_uuid,
       st.type                                                                   subject_type_type,
       'RepeatableQuestionGroup'                                                 table_type,
       f.form_type                                                               parent_table_type,
       p.uuid                                                                    program_uuid,
       op.name                                                                   program_name,
       et.uuid                                                                   encounter_type_uuid,
       oet.name                                                                  encounter_type_name,
       fm.enable_approval                                                        enable_approval,
       c.name                                                                    concept_name,
       gc.name                                                                   parent_concept_name,
       c.id                                                                      concept_id,
       gc.id                                                                     parent_concept_id,
       c.uuid                                                                    concept_uuid,
       c.is_voided                                                               concept_voided,
       null                                                                     parent_concept_uuid,
       gc.uuid                                                                   repeatable_question_group_concept_uuid,
       (case when c.data_type = 'Coded' then fe.type else c.data_type end)    as element_type,
       (case when gc.data_type = 'Coded' then gfe.type else gc.data_type end) as parent_element_type
from form_mapping fm
         inner join form f on fm.form_id = f.id
         left outer join form_element_group feg on f.id = feg.form_id
         left outer join form_element fe on feg.id = fe.form_element_group_id and fe.is_voided is false and fe.group_id is not null
         left outer join form_element gfe on gfe.id = fe.group_id and gfe.is_voided is false
         inner join (select id, jsonb_array_elements(key_values) kv from form_element) gfe_kv on gfe_kv.id = gfe.id
         left outer join concept c on fe.concept_id = c.id and c.uuid <> '"%s"'
         left outer join concept gc on gfe.concept_id = gc.id and gc.data_type = 'QuestionGroup'
         inner join subject_type st on fm.subject_type_id = st.id
         inner join operational_subject_type ost on st.id = ost.subject_type_id
         left outer join program p on fm.entity_id = p.id
         left outer join operational_program op on p.id = op.program_id
         left outer join encounter_type et on fm.observations_type_entity_id = et.id
         left outer join operational_encounter_type oet on et.id = oet.encounter_type_id
         left outer join non_applicable_form_element nafe on fe.id = nafe.form_element_id
where fm.is_voided is false
  and (p.id is null or op.id is not null)
  and (et.id is null or oet.id is not null)
  and nafe.id is null
  and fe.id is not null
  and gfe_kv.kv ->> 'key' = 'repeatable'
  and gfe_kv.kv ->> 'value' = 'true'
