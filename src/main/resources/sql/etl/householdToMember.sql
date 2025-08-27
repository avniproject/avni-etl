insert into ${schema_name}.${table_name} ("id", "uuid", "is_voided", "created_by_id", "last_modified_by_id",
                                          "created_date_time", "last_modified_date_time", "organisation_id",
                                          "group_subject_id", "member_subject_id", "role", "Head of household ID", "created_by_username", "last_modified_by_username")
    (
        select
            gs.id,
            gs.uuid,
            gs.is_voided,
            gs.created_by_id,
            gs.last_modified_by_id,
            gs.created_date_time,
            GREATEST(gs.last_modified_date_time, gs2.last_modified_date_time) as last_modified_date_time,
            gs.organisation_id,
            grp.id                                             AS group_subject_id,
            member.id                                          AS member_subject_id,
            rr.role                                            AS role,
            gs2.member_subject_id                            AS head_of_family_id,
            created_user.username                              AS created_by_username,
            modified_user.username                             AS last_modified_by_username
        FROM public.individual member
                 JOIN subject_type st ON member.subject_type_id = st.id AND st.type::text = 'Person'::text
                 JOIN group_subject gs ON gs.member_subject_id = member.id
                 LEFT JOIN public.individual grp ON grp.id = gs.group_subject_id
                 LEFT JOIN group_subject gs2 ON gs2.group_subject_id = grp.id
                 LEFT JOIN group_role gr ON gs2.group_role_id = gr.id
                 JOIN subject_type gst ON grp.subject_type_id = gst.id
                 JOIN group_role rr ON gs.group_role_id = rr.id
                 LEFT JOIN public.users created_user ON gs.created_by_id = created_user.id
                 LEFT JOIN public.users modified_user ON gs.last_modified_by_id = modified_user.id
            AND member.subject_type_id = rr.member_subject_type_id
            AND grp.subject_type_id = rr.group_subject_type_id
        WHERE NOT gs2.is_voided
          AND st.uuid = '${member_subject_type_uuid}'
          AND gst.uuid = '${group_subject_type_uuid}'
          AND (gr.role = 'Head of household'::text OR gr.role IS NULL)
          AND ((gs.last_modified_date_time > '${start_time}' AND gs.last_modified_date_time <= '${end_time}')
                OR
              (gs2.last_modified_date_time > '${start_time}' AND gs2.last_modified_date_time <= '${end_time}')));