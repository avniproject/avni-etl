--[SQL template for auto generated view]
-- One row per approval decision (#174), not one per record: a record judged twice appears twice, so a
-- report can count rejections by reason. Nothing here may assume one row per entity.
--
-- entity_type and entity_type_uuid are resolved in Java from the mapping shape - see
-- TransactionalSyncSqlGenerator.approvalEntityType. They cannot be written in, because one template
-- serves all four shapes an approval form can attach to.
--
-- The two aliases are contractual, not stylistic. TransactionDataSyncHelper.buildObservationSelection
-- emits entity.observations for form answers and ind.observations for the sync-attribute columns that
-- SchemaMetadataRepository adds to every non-subject table, so both must resolve.
insert into "${schema_name}"."${table_name}" (
    "id", "uuid", "individual_id", "address_id", "entity_id", "entity_type", "entity_type_uuid",
    "approval_status", "status_date_time", "approval_status_comment", "auto_approved",
    "is_voided", "created_by_id", "last_modified_by_id", "created_date_time", "last_modified_date_time",
    "organisation_id"
    ${observations_to_insert_list}
)
(${concept_maps}
SELECT entity.id                                                                           "id",
       entity.uuid                                                                         "uuid",
       entity.individual_id                                                                "individual_id",
       entity.address_id                                                                   "address_id",
       entity.entity_id                                                                    "entity_id",
       entity.entity_type                                                                  "entity_type",
       entity.entity_type_uuid                                                             "entity_type_uuid",
       aps.status                                                                          "approval_status",
       entity.status_date_time                                                             "status_date_time",
       entity.approval_status_comment                                                      "approval_status_comment",
       entity.auto_approved                                                                "auto_approved",
       entity.is_voided                                                                    "is_voided",
       entity.created_by_id                                                                "created_by_id",
       entity.last_modified_by_id                                                          "last_modified_by_id",
       entity.created_date_time                                                            "created_date_time",
       entity.last_modified_date_time                                                      "last_modified_date_time",
       entity.organisation_id                                                              "organisation_id"
       ${selections}
FROM public.entity_approval_status entity
    JOIN public.approval_status aps on entity.approval_status_id = aps.id
    LEFT OUTER JOIN public.individual ind on entity.individual_id = ind.id
    LEFT OUTER JOIN public.subject_type st on st.id = ind.subject_type_id
  ${cross_join_concept_maps}
WHERE entity.entity_type = '${approval_entity_type}'
  AND entity.entity_type_uuid = '${approval_entity_type_uuid}'
  AND st.uuid = '${subject_type_uuid}'
  AND aps.status = 'Rejected'
  and entity.last_modified_date_time > '${start_time}'
  and entity.last_modified_date_time <= '${end_time}'
    );
