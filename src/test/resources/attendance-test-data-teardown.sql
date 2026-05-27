-- Remove attendance/calendar rows before test-data-teardown.sql deletes individuals/org.
delete from attendance_record;
delete from session;
delete from calendar_date_marker;
delete from calendar;
delete from attendance_type;
delete from group_subject;
delete from group_role;
update subject_type set attendance_enabled = false where id = 340;
