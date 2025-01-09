-- Person
insert into concept (id, data_type, high_absolute, high_normal, low_absolute, low_normal, name, uuid, version, unit, organisation_id, is_voided, audit_id, key_values, active, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values (108001, 'Image', null, null, null, null, 'Image concept', '44163589-f76d-447d-9b6e-f5c32aa859eb', 0, null, 12, false, create_audit(), null, true, 1, 1, '2022-04-13 15:49:55.019 +00:00', '2022-04-13 15:49:55.019 +00:00');
insert into concept (id, data_type, high_absolute, high_normal, low_absolute, low_normal, name, uuid, version, unit, organisation_id, is_voided, audit_id, key_values, active, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values (108002, 'Image', null, null, null, null, 'Person''s Image','b1815016-40d6-4045-9ae0-2f659fc5db6d', 0, null, 12, false, create_audit(), null, true, 1, 1, '2022-04-13 15:49:55.019 +00:00', '2022-04-13 15:49:55.019 +00:00');
insert into form_element (id, name, display_order, is_mandatory, key_values, concept_id, form_element_group_id, uuid, version, organisation_id, type, valid_format_regex, valid_format_description_key, audit_id, is_voided, rule, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, declarative_rule) values (50116, 'Media form element', 8, false, '[]', 108001, 5569, 'fd5ad16f-c18e-45db-841c-a2f994862957', 0, 12, 'SingleSelect', null, null, create_audit(), false, null, 1, 1, '2022-04-13 10:51:44.705 +00:00', '2022-04-13 10:51:44.705 +00:00', null);
insert into form_element (id, name, display_order, is_mandatory, key_values, concept_id, form_element_group_id, uuid, version, organisation_id, type, valid_format_regex, valid_format_description_key, audit_id, is_voided, rule, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, declarative_rule) values (50117, 'Media form element', (select max(display_order) + 1 from form_element where form_element_group_id = 5569), false, '[]', 108002, 5569, 'ec952777-67b2-47c2-bd7c-8e277547cb6b', 0, 12, 'SingleSelect', null, null, create_audit(), false, null, 1, 1, '2022-04-13 10:51:44.705 +00:00', '2022-04-13 10:51:44.705 +00:00', null);
update individual set observations = observations || jsonb_build_object('44163589-f76d-447d-9b6e-f5c32aa859eb', 'https://s3.amazon.com/wonderfulImage1.jpg'), last_modified_date_time = '2022-04-13 10:51:44.705 +00:00' where subject_type_id = 339;
update individual set observations = observations || jsonb_build_object('b1815016-40d6-4045-9ae0-2f659fc5db6d', 'https://s3.amazon.com/wonderfulImage2.jpg'), last_modified_date_time = '2022-04-13 10:51:44.705 +00:00' where subject_type_id = 339;


-- Encounter
insert into form_element_group (id, name, form_id, uuid, version, display_order, organisation_id, audit_id, is_voided, rule, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, declarative_rule) values (50571, 'Page', 2660, 'c28ba4eb-f159-4b18-a7c1-baf23d6f7259', 0, 10, 12, create_audit(), false, null, 1, 1, '2022-03-16 15:51:44.666 +00:00', '2022-03-16 15:51:44.666 +00:00', null);
insert into form_element (id, name, display_order, is_mandatory, key_values, concept_id, form_element_group_id, uuid, version, organisation_id, type, valid_format_regex, valid_format_description_key, audit_id, is_voided, rule, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, declarative_rule) values (50115, 'Media form element', 1, false, '[]', 108001, 50571, '3ae42c9b-136f-40da-bd9b-2ff638e55683', 0, 12, 'SingleSelect', null, null, create_audit(), false, null, 1, 1, '2022-04-13 10:51:44.705 +00:00', '2022-04-13 10:51:44.705 +00:00', null);


update encounter set observations = observations || jsonb_build_object('44163589-f76d-447d-9b6e-f5c32aa859eb', 'https://s3.amazon.com/encounterImage1.jpg'), last_modified_date_time = '2022-04-13 10:51:44.705 +00:00' where id = 1900;

