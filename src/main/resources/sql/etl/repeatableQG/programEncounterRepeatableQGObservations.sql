--[SQL template for auto generated view]
insert into ${schema_name}.${table_name} (
    "individual_id", "program_enrolment_id", "program_encounter_id", "address_id", "is_voided", "organisation_id", "last_modified_date_time", "repeatable_question_group_index"
    ${observations_to_insert_list}
)
${concept_maps}
SELECT entity.individual_id                                                                "individual_id",
   entity.program_enrolment_id                                                             "program_enrolment_id",
       entity.program_encounter_id                                                         "program_encounter_id",
       entity.address_id                                                                   "address_id",
       entity.is_voided                                                                    "is_voided",
       entity.organisation_id                                                              "organisation_id",
       entity.last_modified_date_time                                                      "last_modified_date_time",
       entity.repeatable_question_group_index                                              "repeatable_question_group_index"
       ${selections}
FROM (
    select
        rqg.value as observations,
        (rqg.ordinality - 1) as repeatable_question_group_index,
        mainTable.id as program_encounter_id,
        mainTable.program_enrolment_id as program_enrolment_id,
        enrolment.individual_id as individual_id,
        mainTable.address_id as address_id,
        mainTable.is_voided as is_voided,
        mainTable.organisation_id as organisation_id,
        mainTable.last_modified_date_time as last_modified_date_time
    from public.program_encounter mainTable
    inner join public.program_enrolment enrolment on mainTable.program_enrolment_id = enrolment.id
    inner join public.individual ind on enrolment.individual_id = ind.id
    inner join public.encounter_type et on mainTable.encounter_type_id = et.id
    inner join public.program p on enrolment.program_id = p.id
    inner join public.subject_type st on ind.subject_type_id = st.id
    cross join lateral jsonb_array_elements((mainTable.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb) with ordinality as rqg(value, ordinality)
    where et.uuid = '${encounter_type_uuid}'
    and st.uuid = '${subject_type_uuid}'
    and p.uuid = '${program_uuid}'
    and mainTable.observations ->> '${repeatable_question_group_concept_uuid}' is not null
    and jsonb_typeof((mainTable.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb) = 'array'
    and jsonb_array_length((mainTable.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb) > 0
    and mainTable.cancel_date_time isnull
    and mainTable.last_modified_date_time > '${start_time}'
    and mainTable.last_modified_date_time <= '${end_time}'
) entity
${cross_join_concept_maps}
;
