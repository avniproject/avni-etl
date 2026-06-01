-- Attendance/calendar fixture for org 12 (schema orgc), layered on test-data.sql.
-- Group subject: individual 574171 (Household, subject_type 340) at address 107786 (lineage 107782.107784.107786).
-- Members: 574170, 574173, 574174 (Person, subject_type 339, address 107786).
-- Calendar cal_bengaluru is attached at address_level 107782 (the District), so the group at the
-- Gram-Panchayat level resolves to it by walking up the address hierarchy.

-- A second attendance group subject type "Class" (type Group, distinct from the Household type 340),
-- with its operational type, registration form and form mapping so the ETL generates a second group
-- subject table. This makes expected_sessions iterate more than one group table (Group + Household)
-- via UNION ALL.
INSERT INTO subject_type (id, uuid, name, organisation_id, is_voided, audit_id, version, is_group, is_household, active, type, subject_summary_rule, allow_empty_location, unique_name, valid_first_name_regex, valid_first_name_description_key, valid_last_name_regex, valid_last_name_description_key, icon_file_s3_key, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time)
VALUES (9001, 'st-class-0000-0000-0000-000000009001', 'Class', 12, false, create_audit(), 0, true, false, true, 'Group', '', false, false, null, null, null, null, null, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00');
INSERT INTO operational_subject_type (id, uuid, name, subject_type_id, organisation_id, is_voided, audit_id, version, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time)
VALUES (9050, 'ost-class-0000-0000-0000-00000009050', 'Class', 9001, 12, false, create_audit(), 0, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00');
INSERT INTO form (id, name, form_type, uuid, version, organisation_id, audit_id, is_voided, decision_rule, validation_rule, visit_schedule_rule, checklists_rule, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, validation_declarative_rule, decision_declarative_rule, visit_schedule_declarative_rule)
VALUES (9060, 'Class Registration', 'IndividualProfile', 'form-class-0000-0000-0000-00000009060', 0, 12, create_audit(), false, null, null, null, null, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', null, null, null);
INSERT INTO form_mapping (id, form_id, uuid, version, entity_id, observations_type_entity_id, organisation_id, audit_id, is_voided, subject_type_id, enable_approval, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time)
VALUES (9070, 9060, 'fm-class-0000-0000-0000-000000009070', 0, null, null, 12, create_audit(), false, 9001, false, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00');

UPDATE subject_type SET attendance_enabled = true WHERE id IN (340, 9001);

-- A second Household group (574180) under the Mysuru subtree (107787, lineage 107783.107785.107787),
-- which has no non-voided calendar in its hierarchy and no org global default -> calendar unresolved.
INSERT INTO individual (id, uuid, address_id, observations, version, date_of_birth, date_of_birth_verified, gender_id, registration_date, organisation_id, first_name, last_name, is_voided, audit_id, facility_id, registration_location, subject_type_id, legacy_id, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, sync_concept_1_value, sync_concept_2_value)
VALUES (574180, 'household-unresolved-0000-0000-00000180', 107787, '{}', 0, null, false, null, '2024-03-01', 12, 'Unresolved Group', null, false, create_audit(), null, null, 340, null, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', null, null);

-- A Class group (574190) at address 107786 -> resolves to the Bengaluru calendar via walk-up.
INSERT INTO individual (id, uuid, address_id, observations, version, date_of_birth, date_of_birth_verified, gender_id, registration_date, organisation_id, first_name, last_name, is_voided, audit_id, facility_id, registration_location, subject_type_id, legacy_id, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, sync_concept_1_value, sync_concept_2_value)
VALUES (574190, 'class-group-0000-0000-0000-00000000190', 107786, '{}', 0, null, false, null, '2024-03-01', 12, 'Class A', null, false, create_audit(), null, null, 9001, null, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', null, null);

-- Calendars: a per-location calendar (Bengaluru, sat working on 1st/3rd/5th) and a voided one.
-- No org global default, so a group outside any calendar subtree (574180) stays unresolved.
INSERT INTO calendar (id, uuid, name, working_pattern, address_level_id, is_default, organisation_id, is_voided, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, version) VALUES
 (90001, 'cal-bengaluru-0000-0000-000000000001', 'Bengaluru Schools', '{"mon":"all","tue":"all","wed":"all","thu":"all","fri":"all","sat":[1,3,5],"sun":"none"}', 107782, false, 12, false, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 0),
 (90003, 'cal-voided-0000-0000-0000-000000000003', 'Voided Calendar', '{"mon":"all","tue":"all","wed":"all","thu":"all","fri":"all","sat":"none","sun":"none"}', 107783, false, 12, true, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 0);

-- Markers on the Bengaluru calendar: a Monday public holiday, a Sunday working override, and a voided marker.
INSERT INTO calendar_date_marker (id, uuid, calendar_id, marker_date, name, is_working, organisation_id, is_voided, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, version) VALUES
 (91001, 'marker-holiday-0000-0000-000000000001', 90001, '2024-03-11', 'Republic Holiday', false, 12, false, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 0),
 (91002, 'marker-override-0000-0000-00000000002', 90001, '2024-03-10', 'Makeup Day', true, 12, false, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 0),
 (91003, 'marker-voided-0000-0000-0000-000000003', 90001, '2024-03-12', 'Voided Marker', false, 12, true, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 0);

-- Attendance types for the Household subject type (one active with config, one voided).
INSERT INTO attendance_type (id, uuid, subject_type_id, name, sort_order, config, organisation_id, is_voided, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, version) VALUES
 (92001, 'attype-morning-0000-0000-000000000001', 340, 'Morning', 1, '{"follow_up_encounter_type_uuid":"fu-encounter-uuid","auto_share_on_save":true}', 12, false, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 0),
 (92002, 'attype-evening-0000-0000-000000000002', 340, 'Evening', 2, '{}', 12, true, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 0),
 (92010, 'attype-class-0000-0000-0000-000000092010', 9001, 'ClassSession', 1, '{}', 12, false, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 0);

-- Group membership: 574170 and 574173 active; 574174 left before March (membership window test).
INSERT INTO group_role (id, uuid, group_subject_type_id, role, member_subject_type_id, is_primary, maximum_number_of_members, minimum_number_of_members, organisation_id, audit_id, is_voided, version, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) VALUES
 (93001, 'grouprole-0000-0000-0000-000000000001', 340, 'Member', 339, true, 100, 0, 12, create_audit(), false, 0, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00');

INSERT INTO group_subject (id, uuid, group_subject_id, member_subject_id, group_role_id, membership_start_date, membership_end_date, organisation_id, audit_id, is_voided, version, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, member_subject_address_id, group_subject_address_id) VALUES
 (94001, 'groupsub-0000-0000-0000-0000000000001', 574171, 574170, 93001, '2024-01-01 00:00:00+00', null, 12, create_audit(), false, 0, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 107786, 107786),
 (94002, 'groupsub-0000-0000-0000-0000000000002', 574171, 574173, 93001, '2024-01-01 00:00:00+00', null, 12, create_audit(), false, 0, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 107786, 107786),
 (94003, 'groupsub-0000-0000-0000-0000000000003', 574171, 574174, 93001, '2024-01-01 00:00:00+00', '2024-02-15 00:00:00+00', 12, create_audit(), false, 0, 1, 1, '2024-03-01 10:00:00+00', '2024-03-01 10:00:00+00', 107786, 107786);

-- Sessions for the group on the Morning attendance type.
INSERT INTO session (id, uuid, group_subject_id, scheduled_date, attendance_type_id, status, reason_concept_id, notes, marked_by_user_id, marked_at, organisation_id, is_voided, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, version) VALUES
 (95001, 'session-0000-0000-0000-00000000000001', 574171, '2024-03-04', 92001, 'Held', null, null, 3453, '2024-03-04 12:00:00+00', 12, false, 1, 1, '2024-03-04 10:00:00+00', '2024-03-04 10:00:00+00', 0),
 (95002, 'session-0000-0000-0000-00000000000002', 574171, '2024-03-16', 92001, 'Held', null, null, 3453, '2024-03-16 12:00:00+00', 12, false, 1, 1, '2024-03-16 10:00:00+00', '2024-03-16 10:00:00+00', 0),
 (95003, 'session-0000-0000-0000-00000000000003', 574171, '2024-03-09', 92001, 'Held', null, null, 3453, '2024-03-09 12:00:00+00', 12, false, 1, 1, '2024-03-09 10:00:00+00', '2024-03-09 10:00:00+00', 0),
 (95004, 'session-0000-0000-0000-00000000000004', 574171, '2024-03-20', 92001, 'Held', null, null, 3453, '2024-03-20 12:00:00+00', 12, true, 1, 1, '2024-03-20 10:00:00+00', '2024-03-20 10:00:00+00', 0),
 -- Off-day session for the unresolved group (574180): must NOT appear in expected_sessions (incl. mark_anyway).
 (95005, 'session-0000-0000-0000-00000000000005', 574180, '2024-03-09', 92001, 'Held', null, null, 3453, '2024-03-09 12:00:00+00', 12, false, 1, 1, '2024-03-09 10:00:00+00', '2024-03-09 10:00:00+00', 0),
 -- Off-day session using the VOIDED attendance type (92002): must NOT appear in expected_sessions (incl. mark_anyway).
 (95006, 'session-0000-0000-0000-00000000000006', 574171, '2024-03-09', 92002, 'Held', null, null, 3453, '2024-03-09 12:00:00+00', 12, false, 1, 1, '2024-03-09 10:00:00+00', '2024-03-09 10:00:00+00', 0),
 -- Held session for the Class group (574190) on a working day: exercises the second group subject table.
 (95010, 'session-0000-0000-0000-00000000000010', 574190, '2024-03-04', 92010, 'Held', null, null, 3453, '2024-03-04 12:00:00+00', 12, false, 1, 1, '2024-03-04 10:00:00+00', '2024-03-04 10:00:00+00', 0);

-- Attendance records for the 2024-03-04 session: present + absent, plus a voided record.
INSERT INTO attendance_record (id, uuid, session_id, subject_id, status, follow_up_encounter_uuid, organisation_id, is_voided, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, version) VALUES
 (96001, 'attrec-0000-0000-0000-0000000000001', 95001, 574170, 'Present', null, 12, false, 1, 1, '2024-03-04 12:30:00+00', '2024-03-04 12:30:00+00', 0),
 (96002, 'attrec-0000-0000-0000-0000000000002', 95001, 574173, 'Absent', null, 12, false, 1, 1, '2024-03-04 12:30:00+00', '2024-03-04 12:30:00+00', 0),
 (96003, 'attrec-0000-0000-0000-0000000000003', 95001, 574174, 'Present', null, 12, true, 1, 1, '2024-03-04 12:30:00+00', '2024-03-04 12:30:00+00', 0);
