drop schema if exists ogi1 cascade ;
drop schema if exists ogi2 cascade ;
drop schema if exists og cascade ;

delete from organisation where id = 13;
delete from organisation where id = 14;
delete from organisation_group where id = 11;
delete from organisation_group_organisation where id = 11;
delete from organisation_group_organisation where id = 12;
delete from address_level_type where organisation_id in (13, 14);
delete from address_level where organisation_id in (13, 14);
delete from users where organisation_id in (13, 14);
delete from subject_type where organisation_id in (13, 14);
delete from operational_subject_type where organisation_id in (13, 14);
delete from program where organisation_id in (13, 14);
delete from operational_program where organisation_id in (13, 14);
delete from encounter_type where organisation_id in (13, 14);
delete from operational_encounter_type where organisation_id in (13, 14);
delete from concept where organisation_id in (13, 14);
delete from concept_answer where organisation_id in (13, 14);
delete from form where organisation_id in (13, 14);
delete from form_element_group where organisation_id in (13, 14);
delete from form_element where organisation_id in (13, 14);
delete from form_mapping where organisation_id in (13, 14);
delete from decision_concept where form_id in (select id from form where organisation_id in (13, 14));
delete from gender where organisation_id in (13, 14);
delete from individual where organisation_id in (13, 14);
delete from program_enrolment where organisation_id in (13, 14);
delete from program_encounter where organisation_id in (13, 14);
delete from encounter where organisation_id in (13, 14);
delete from entity_approval_status where organisation_id in (13, 14);
