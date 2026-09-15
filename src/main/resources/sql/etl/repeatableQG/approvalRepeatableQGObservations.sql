--[SQL template for auto generated view]
-- A repeatable question group asked on an Approval form (#174).
--
-- The parent is the decision, not the record judged, so rows hang off entity_approval_status_id and a
-- record judged twice contributes two sets of question-group rows.
--
-- The filters mirror approval.sql exactly: entity_type, entity_type_uuid and the programme filter are
-- resolved in Java because one template serves all four mapping shapes a decision form can attach to.
--
-- The inner alias is contractual, not stylistic. TransactionDataSyncHelper.approvalProgramFilter emits
-- "entity.entity_id", so the table it filters has to be called entity here just as it is in approval.sql.
insert into "${schema_name}"."${table_name}" (
    "individual_id", "address_id", "entity_approval_status_id", "is_voided", "organisation_id", "last_modified_date_time", "repeatable_question_group_index"
    ${observations_to_insert_list}
)
${concept_maps}
SELECT entity.individual_id                                                                "individual_id",
       entity.address_id                                                                   "address_id",
       entity.entity_approval_status_id                                                    "entity_approval_status_id",
       entity.is_voided                                                                    "is_voided",
       entity.organisation_id                                                              "organisation_id",
       entity.last_modified_date_time                                                      "last_modified_date_time",
       entity.repeatable_question_group_index                                              "repeatable_question_group_index"
       ${selections}
FROM (
    select
        rqg.value as observations,
        (rqg.ordinality - 1) as repeatable_question_group_index,
        entity.id as entity_approval_status_id,
        entity.individual_id as individual_id,
        entity.address_id as address_id,
        entity.is_voided as is_voided,
        entity.organisation_id as organisation_id,
        entity.last_modified_date_time as last_modified_date_time
    from public.entity_approval_status entity
    join public.approval_status aps on entity.approval_status_id = aps.id
    left outer join public.individual ind on entity.individual_id = ind.id
    left outer join public.subject_type st on st.id = ind.subject_type_id
    cross join lateral jsonb_array_elements((entity.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb) with ordinality as rqg(value, ordinality)
    where entity.entity_type = '${approval_entity_type}'
    and entity.entity_type_uuid = '${approval_entity_type_uuid}'
    ${approval_program_filter}
    and st.uuid = '${subject_type_uuid}'
    and aps.status = 'Approved'
    and entity.observations ->> '${repeatable_question_group_concept_uuid}' is not null
    and jsonb_typeof((entity.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb) = 'array'
    and jsonb_array_length((entity.observations ->> '${repeatable_question_group_concept_uuid}')::jsonb) > 0
    and entity.last_modified_date_time > '${start_time}'
    and entity.last_modified_date_time <= '${end_time}'
) entity
${cross_join_concept_maps}
;
