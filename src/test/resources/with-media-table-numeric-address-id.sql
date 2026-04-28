CREATE SCHEMA IF NOT EXISTS orgc;

CREATE TABLE orgc.media
(
    id                               serial primary key,
    uuid                             text,
    is_voided                        bool,
    created_by_id                    integer,
    last_modified_by_id              integer,
    created_date_time                timestamp with time zone,
    last_modified_date_time          timestamp with time zone,
    organisation_id                  integer,
    address_id                       numeric,
    image_url                        text,
    sync_parameter_key1              text,
    sync_parameter_value1            text,
    sync_parameter_key2              text,
    sync_parameter_value2            text,
    subject_type_name                text,
    encounter_type_name              text,
    program_name                     text,
    concept_name                     text,
    entity_id                        integer,
    subject_first_name               text,
    subject_last_name                text,
    subject_middle_name              text,
    media_metadata                   jsonb,
    question_group_concept_name      text,
    repeatable_question_group_index  integer
);

INSERT INTO table_metadata (id, name, type, schema_name, subject_type_uuid, program_uuid, encounter_type_uuid, form_uuid)
VALUES (9000, 'media', 'Media', 'orgc', null, null, null, null);

INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9001, 9000, 'id',                              'serial',                 null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9002, 9000, 'uuid',                            'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9003, 9000, 'is_voided',                       'bool',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9004, 9000, 'created_by_id',                   'integer',                null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9005, 9000, 'last_modified_by_id',             'integer',                null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9006, 9000, 'created_date_time',               'timestampWithTimezone',  null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9007, 9000, 'last_modified_date_time',         'timestampWithTimezone',  null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9008, 9000, 'organisation_id',                 'integer',                null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9009, 9000, 'address_id',                      'numeric',                null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9010, 9000, 'image_url',                       'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9011, 9000, 'sync_parameter_key1',             'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9012, 9000, 'sync_parameter_value1',           'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9013, 9000, 'sync_parameter_key2',             'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9014, 9000, 'sync_parameter_value2',           'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9015, 9000, 'subject_type_name',               'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9016, 9000, 'encounter_type_name',             'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9017, 9000, 'program_name',                    'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9018, 9000, 'concept_name',                    'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9019, 9000, 'entity_id',                       'integer',                null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9020, 9000, 'subject_first_name',              'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9021, 9000, 'subject_last_name',               'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9022, 9000, 'subject_middle_name',             'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9023, 9000, 'media_metadata',                  'jsonb',                  null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9024, 9000, 'question_group_concept_name',     'text',                   null, null, null, 'orgc');
INSERT INTO column_metadata (id, table_id, name, type, concept_id, concept_type, concept_uuid, schema_name) VALUES (9025, 9000, 'repeatable_question_group_index', 'integer',                null, null, null, 'orgc');

INSERT INTO orgc.media (uuid, address_id) VALUES ('test-uuid-1', 42);
INSERT INTO orgc.media (uuid, address_id) VALUES ('test-uuid-2', 101);
