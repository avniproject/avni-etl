insert into ${schema_name}.${table_name} ("id", "uuid", "is_voided", "created_by_id", "last_modified_by_id",
                                          "created_date_time", "last_modified_date_time", "organisation_id",
                                          "group_subject_id", "member_subject_id", "role")
    (
        select
            gs.id,
            gs.uuid,
            gs.is_voided,
            gs.created_by_id,
            gs.last_modified_by_id,
            gs.created_date_time,
            gs.last_modified_date_time,
            gs.organisation_id,
            grp.id                                             AS group_subject_id,
            member.id                                          AS member_subject_id,
            rr.role                                            AS role
        FROM public.individual member
                 JOIN subject_type st ON member.subject_type_id = st.id AND st.type::text in ('Person'::text, 'Individual'::text)
                 JOIN group_subject gs ON gs.member_subject_id = member.id
                 LEFT JOIN public.individual grp ON grp.id = gs.group_subject_id
                 JOIN subject_type gst ON grp.subject_type_id = gst.id
                 JOIN group_role rr ON gs.group_role_id = rr.id
                    AND member.subject_type_id = rr.member_subject_type_id
                    AND grp.subject_type_id = rr.group_subject_type_id
        WHERE st.uuid = '${member_subject_type_uuid}'
          AND gst.uuid = '${group_subject_type_uuid}'
          AND gs.last_modified_date_time > '${start_time}'
          AND gs.last_modified_date_time <= '${end_time}');