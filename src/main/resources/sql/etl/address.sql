insert into "${schema_name}".address ("${titleColumnName}", "${idColumnName}", id, "uuid", "is_voided", "created_by_id", "last_modified_by_id",
                                    "created_date_time", "last_modified_date_time", "organisation_id", "gps_coordinates"
                                        ${observations_to_insert_list})
(${concept_maps}
    select entity.title,
    entity.id,
    entity.id,
    entity.uuid,
    entity.is_voided,
    entity.created_by_id,
    entity.last_modified_by_id,
    entity.created_date_time,
    entity.last_modified_date_time,
    entity.organisation_id,
    entity.gps_coordinates
       ${selections}
from address_level entity
         join address_level_type alt on entity.type_id = alt.id
         ${cross_join_concept_maps}
where alt.name = '${titleColumnName}'
  and entity.last_modified_date_time > '${start_time}'
  and entity.last_modified_date_time <= '${end_time}');
