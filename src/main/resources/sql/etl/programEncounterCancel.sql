--[SQL template for auto generated view]
insert into ${schema_name}.${table_name} (
    "program_enrolment_id",
    "id",
    "individual_id",
    "earliest_visit_date_time",
    "encounter_date_time",
    "uuid",
    "name",
    "address_id",
    "max_visit_date_time",
    "is_voided",
    "encounter_location",
    "legacy_id",
    "cancel_date_time",
    "cancel_location",
    "created_by_id",
    "last_modified_by_id",
    "filled_by_id",
    "created_date_time",
    "last_modified_date_time",
    "organisation_id",
    "created_by_username",
    "last_modified_by_username",
    "filled_by_username"
        ${observations_to_insert_list}
)
    (${concept_maps}
        SELECT entity.program_enrolment_id                                                  "program_enrolment_id",
        entity.id                                                                    "id",
        entity.individual_id                                                         "individual_id",
        entity.earliest_visit_date_time                                              "earliest_visit_date_time",
        entity.encounter_date_time                                                   "encounter_date_time",
        entity.uuid                                                                  "uuid",
        entity.name                                                                  "name",
        entity.address_id                                                            "address_id",
        entity.max_visit_date_time                                                   "max_visit_date_time",
        entity.is_voided                                                             "is_voided",
        entity.encounter_location                                                    "encounter_location",
        entity.legacy_id                                                             "legacy_id",
        entity.cancel_date_time                                                      "cancel_date_time",
        entity.cancel_location                                                       "cancel_location",
        entity.created_by_id                                                         "created_by_id",
        entity.last_modified_by_id                                                   "last_modified_by_id",
        entity.filled_by_id                                                          "filled_by_id",
        entity.created_date_time                                                     "created_date_time",
        entity.last_modified_date_time                                               "last_modified_date_time",
        entity.organisation_id                                                       "organisation_id",
        created_user.username                                                        "created_by_username",
        modified_user.username                                                       "last_modified_by_username",
        filled_user.username                                                         "filled_by_username"
        ${cancel_obs_selections}
        FROM public.program_encounter entity
        ${cross_join_concept_maps}
        LEFT OUTER JOIN public.program_enrolment programEnrolment
        ON entity.program_enrolment_id = programEnrolment.id
        LEFT OUTER JOIN public.individual ind on programEnrolment.individual_id = ind.id
        LEFT OUTER JOIN public.encounter_type et on entity.encounter_type_id = et.id
        LEFT OUTER JOIN public.subject_type st on st.id = ind.subject_type_id
        LEFT OUTER JOIN public.program p on p.id = programEnrolment.program_id
        LEFT OUTER JOIN public.users created_user on entity.created_by_id = created_user.id
        LEFT OUTER JOIN public.users modified_user on entity.last_modified_by_id = modified_user.id
        LEFT OUTER JOIN public.users filled_user on entity.filled_by_id = filled_user.id
        WHERE p.uuid = '${program_uuid}'
        AND et.uuid = '${encounter_type_uuid}'
        AND st.uuid = '${subject_type_uuid}'
        AND entity.cancel_date_time is not null
        and entity.last_modified_date_time > '${start_time}'
        and entity.last_modified_date_time <= '${end_time}');
