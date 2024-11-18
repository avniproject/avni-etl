--[SQL template for auto generated view]
insert into ${schema_name}.${table_name} (
    "individual_id", "program_enrolment_id", "address_id", "is_voided", "organisation_id", "last_modified_date_time"
    ${observations_to_insert_list}
)
(${concept_maps}
SELECT entity.individual_id                                                                "individual_id",
       entity.program_enrolment_id                                                         "program_enrolment_id",
       entity.address_id                                                                   "address_id",
       entity.is_voided                                                                    "is_voided",
       entity.organisation_id                                                              "organisation_id",
       entity.last_modified_date_time                                                      "last_modified_date_time"
       ${selections}
FROM (
    select jsonb_array_elements((mainTable.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb)  as observations,
    individual_id                                                                                     as individual_id,
    mainTable.id                                                                                     as program_enrolment_id,
    mainTable.address_id                                                                             as address_id,
    mainTable.is_voided                                                                              as is_voided,
    mainTable.organisation_id                                                                        as organisation_id,
    mainTable.last_modified_date_time                                                                as last_modified_date_time
    from public.program_enrolment mainTable
    inner join public.individual ind on mainTable.individual_id = ind.id
    inner join public.program p on mainTable.program_id = p.id
    inner join public.subject_type st on ind.subject_type_id = st.id
    where p.uuid = '${program_uuid}'
    and mainTable.observations ->> '${repeatable_question_group_concept_uuid}' is not null
    and st.uuid = '${subject_type_uuid}'
    and jsonb_typeof((mainTable.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb) = 'array'
    and jsonb_array_length((mainTable.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb) > 0
    and mainTable.last_modified_date_time > '${start_time}'
    and mainTable.last_modified_date_time <= '${end_time}'
    ) entity
${cross_join_concept_maps}
    );
