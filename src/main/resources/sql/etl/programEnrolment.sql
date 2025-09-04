--[SQL template for auto generated view]
insert into ${schema_name}.${table_name} (
    "id",
    "individual_id",
    "uuid",
    "address_id",
    "enrolment_date_time",
    "enrolment_location",
    "is_voided",
    "program_exit_date_time",
    "exit_location",
    "created_by_id",
    "last_modified_by_id",
    "created_date_time",
    "last_modified_date_time",
    "organisation_id",
    "legacy_id"
        ${observations_to_insert_list}
)
(${concept_maps}
SELECT entity.id                                                                    "id",
       ind.id                                                                          "individual_id",
       entity.uuid                                                                  "uuid",
       entity.address_id                                                            "address_id",
       entity.enrolment_date_time                                                   "enrolment_date_time",
       entity.enrolment_location                                                    "enrolment_location",
       entity.is_voided                                                             "is_voided",
       entity.program_exit_date_time                                                "program_exit_date_time",
       entity.exit_location                                                         "exit_location",
       entity.created_by_id                                                         "created_by_id",
       entity.last_modified_by_id                                                   "last_modified_by_id",
       entity.created_date_time                                                     "created_date_time",
       entity.last_modified_date_time                                               "last_modified_date_time",
       entity.organisation_id                                                       "organisation_id",
       entity.legacy_id                                                             "legacy_id"
       ${selections}
FROM public.program_enrolment entity
  ${cross_join_concept_maps}
         LEFT OUTER JOIN public.individual ind ON entity.individual_id = ind.id
    LEFT OUTER JOIN public.subject_type st on st.id = ind.subject_type_id
    LEFT OUTER JOIN public.program p on p.id = entity.program_id
WHERE p.uuid = '${program_uuid}'
  AND st.uuid = '${subject_type_uuid}'
    and entity.last_modified_date_time > '${start_time}'
    and entity.last_modified_date_time <= '${end_time}');
