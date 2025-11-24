--[SQL template for auto generated view]
insert into "${schema_name}"."${table_name}" (
    "subject_id", "id", "program_id", "is_eligible", "uuid", "check_date", "is_voided",
    "created_by_id", "last_modified_by_id", "created_date_time", "last_modified_date_time", "organisation_id"
    ${observations_to_insert_list}
)
(${concept_maps}
SELECT entity.subject_id                                                                   "subject_id",
       entity.id                                                                           "id",
       entity.program_id                                                                    "program_id",
       entity.is_eligible                                                                   "is_eligible",
       entity.uuid                                                                          "uuid",
       entity.check_date                                                                    "check_date",
       entity.is_voided                                                                    "is_voided",
       entity.created_by_id                                                                "created_by_id",
       entity.last_modified_by_id                                                          "last_modified_by_id",
       entity.created_date_time                                                            "created_date_time",
       entity.last_modified_date_time                                                      "last_modified_date_time",
       entity.organisation_id                                                              "organisation_id"
       ${selections}
FROM public.subject_program_eligibility entity
    LEFT OUTER JOIN public.individual ind on entity.subject_id = ind.id
    LEFT OUTER JOIN public.subject_type st on st.id = ind.subject_type_id
  ${cross_join_concept_maps}
WHERE st.uuid = '${subject_type_uuid}'
    and entity.last_modified_date_time > '${start_time}'
    and entity.last_modified_date_time <= '${end_time}'
    );
