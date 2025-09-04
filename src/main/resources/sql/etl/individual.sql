--[Data Extract Report] Registration
insert into ${schema_name}.${table_name} (
    "id", "address_id", "uuid", "first_name", "last_name", "registration_date", "registration_location",
    "is_voided", "created_by_id", "last_modified_by_id", "created_date_time",
    "last_modified_date_time", "organisation_id", "legacy_id"
        ${observations_to_insert_list}
)
    (${concept_maps}
        SELECT entity.id                                                                as "id",
        entity.address_id                                                               as "address_id",
        entity.uuid                                                                     as "uuid",
        entity.first_name                                                               as "first_name",
        entity.last_name                                                                as "last_name",
        entity.registration_date                                                        as "registration_date",
        entity.registration_location                                                    as "registration_location",
        entity.is_voided                                                                as "is_voided",
        entity.created_by_id                                                            as "created_by_id",
        entity.last_modified_by_id                                                      as "last_modified_by_id",
        entity.created_date_time                                                        as "created_date_time",
        entity.last_modified_date_time                                                  as "last_modified_date_time",
        entity.organisation_id                                                          as "organisation_id",
        entity.legacy_id                                                                as "legacy_id"
        ${selections}
        FROM public.individual entity
        LEFT OUTER JOIN public.subject_type st on st.id = entity.subject_type_id
        ${cross_join_concept_maps}
        LEFT OUTER JOIN public.gender g ON g.id = entity.gender_id
        LEFT OUTER JOIN public.address_level a ON entity.address_id = a.id
        where st.uuid = '${subject_type_uuid}'
        and entity.last_modified_date_time > '${start_time}'
        and entity.last_modified_date_time <= '${end_time}')
