package org.avniproject.etl;

import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.service.EtlService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.jdbc.Sql;

import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * End-to-end verification that the attendance/calendar source tables sync into the org schema
 * as passthrough tables, and that the materialized views derive the documented behaviours
 * (the "gotchas" from issue #168).
 *
 * Fixture (see attendance-test-data.sql): group subject 574171 (Household, address 107786 under
 * lineage 107782.107784.107786); calendar "cal-bengaluru" attached at the District (107782) with
 * working pattern sat:[1,3,5]; a Sunday working-override marker and a Monday public-holiday marker.
 */
// Wide per-student window so the fixed historical fixture dates fall inside per_student_attendance.
@TestPropertySource(properties = "avni.attendance.perStudent.windowInMonths=2400")
public class AttendanceSyncIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private EtlService etlService;


    private static final String BENGALURU_CAL = "cal-bengaluru-0000-0000-000000000001";
    private static final String MORNING_TYPE = "attype-morning-0000-0000-000000000001";
    private static final String DISTRICT_107782_UUID = "fccb202d-e5df-4a23-ad68-3fb0e88c8821";
    private static final String GROUP_574171 = "badfdedc-a6f8-4f8f-bb2f-d4f24c7449c9";
    private static final String GROUP_574180_UNRESOLVED = "household-unresolved-0000-0000-00000180";
    private static final String EVENING_TYPE_VOIDED = "attype-evening-0000-0000-000000000002";
    private static final String CLASS_GROUP_574190 = "class-group-0000-0000-0000-00000000190";
    private static final String CLASS_TYPE = "attype-class-0000-0000-0000-000000092010";
    private static final String WHITE_CONCEPT = "c6012f8d-d705-4d72-a0ba-45d8d37c730b";
    private static final String PERSON_574170 = "751bb8c8-ef18-4250-a73d-73106e7a5b56";
    private static final String PERSON_574173 = "335fec0f-958c-4cc0-b477-c6a5a9ba986b";
    private static final String PERSON_574174 = "b1a62475-61ae-42a2-923f-43df4c747b0d";

    private void runEtl() {
        etlService.runFor(OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc"));
    }

    private String dayType(String date) {
        return jdbcTemplate.queryForObject(format(
                "select day_type from orgc.working_day_calendar where calendar_uuid = '%s' and date = '%s'", BENGALURU_CAL, date), String.class);
    }

    @Test
    @Sql({"/attendance-test-data-teardown.sql", "/test-data-teardown.sql", "/test-data.sql", "/attendance-test-data.sql"})
    @Sql(scripts = {"/attendance-test-data-teardown.sql", "/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void attendanceTablesSyncIntoOrgSchemaIncludingVoided() {
        runEtl();

        // All rows projected, including voided ones (so deletions propagate downstream).
        assertThat(countOfRowsIn("orgc.calendar"), equalTo(2L));
        assertThat(countOfRowsIn("orgc.calendar_date_marker"), equalTo(3L));
        assertThat(countOfRowsIn("orgc.attendance_type"), equalTo(3L));
        assertThat(countOfRowsIn("orgc.session"), equalTo(7L));
        assertThat(countOfRowsIn("orgc.attendance_record"), equalTo(3L));

        // Voided row is present and flagged.
        assertThat(jdbcTemplate.queryForObject("select is_voided from orgc.calendar where uuid = 'cal-voided-0000-0000-0000-000000000003'", Boolean.class), is(true));

        // FK ids resolved to UUIDs.
        assertThat(jdbcTemplate.queryForObject(format("select address_level_uuid from orgc.calendar where uuid = '%s'", BENGALURU_CAL), String.class), is(DISTRICT_107782_UUID));
        assertThat(jdbcTemplate.queryForObject("select group_subject_uuid from orgc.session where uuid = 'session-0000-0000-0000-00000000000001'", String.class), is(GROUP_574171));
        assertThat(jdbcTemplate.queryForObject("select attendance_type_uuid from orgc.session where uuid = 'session-0000-0000-0000-00000000000001'", String.class), is(MORNING_TYPE));
        assertThat(jdbcTemplate.queryForObject("select subject_uuid from orgc.attendance_record where uuid = 'attrec-0000-0000-0000-0000000000001'", String.class), is(PERSON_574170));

        // config jsonb is queryable with JSON ops.
        assertThat(jdbcTemplate.queryForObject(format("select config->>'follow_up_encounter_type_uuid' from orgc.attendance_type where uuid = '%s'", MORNING_TYPE), String.class), is("fu-encounter-uuid"));
        assertThat(jdbcTemplate.queryForObject(format("select (config->>'auto_share_on_save')::boolean from orgc.attendance_type where uuid = '%s'", MORNING_TYPE), Boolean.class), is(true));
    }

    @Test
    @Sql({"/attendance-test-data-teardown.sql", "/test-data-teardown.sql", "/test-data.sql", "/attendance-test-data.sql"})
    @Sql(scripts = {"/attendance-test-data-teardown.sql", "/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void workingDayCalendarDerivesDayTypes() {
        runEtl();

        // sat:[1,3,5] -> 1st/3rd/5th Saturdays working, 2nd/4th weekly_off (March 2024).
        assertThat(dayType("2024-03-02"), is("working_day"));   // 1st Saturday
        assertThat(dayType("2024-03-09"), is("weekly_off"));    // 2nd Saturday
        assertThat(dayType("2024-03-16"), is("working_day"));   // 3rd Saturday
        assertThat(dayType("2024-03-23"), is("weekly_off"));    // 4th Saturday
        assertThat(dayType("2024-03-30"), is("working_day"));   // 5th Saturday

        // Plain pattern days.
        assertThat(dayType("2024-03-04"), is("working_day"));   // Monday, pattern = all
        assertThat(dayType("2024-03-03"), is("weekly_off"));    // Sunday, pattern = none

        // Markers override the pattern.
        assertThat(dayType("2024-03-10"), is("working_override"));  // Sunday marker is_working=true
        assertThat(dayType("2024-03-11"), is("public_holiday"));    // Monday marker is_working=false

        // marker_name populated only for marker-driven days.
        assertThat(jdbcTemplate.queryForObject(format("select marker_name from orgc.working_day_calendar where calendar_uuid='%s' and date='2024-03-11'", BENGALURU_CAL), String.class), is("Republic Holiday"));
        assertThat(jdbcTemplate.queryForObject(format("select marker_name from orgc.working_day_calendar where calendar_uuid='%s' and date='2024-03-10'", BENGALURU_CAL), String.class), is("Makeup Day"));
        assertThat(jdbcTemplate.queryForObject(format("select marker_name from orgc.working_day_calendar where calendar_uuid='%s' and date='2024-03-04'", BENGALURU_CAL), String.class), is(nullValue()));

        // The voided calendar is excluded from the view.
        assertThat(countOfRowsIn("orgc.working_day_calendar where calendar_uuid = 'cal-voided-0000-0000-0000-000000000003'"), equalTo(0L));
    }

    @Test
    @Sql({"/attendance-test-data-teardown.sql", "/test-data-teardown.sql", "/test-data.sql", "/attendance-test-data.sql"})
    @Sql(scripts = {"/attendance-test-data-teardown.sql", "/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void subjectResolvedCalendarWalksUpHierarchyAndExcludesUnresolved() {
        runEtl();

        // Group is at the Gram-Panchayat level (107786); calendar is attached at the District (107782).
        // Resolution walks up the lineage to the District calendar.
        String resolved = jdbcTemplate.queryForObject(format("select calendar_uuid from orgc.subject_resolved_calendar where subject_uuid = '%s'", GROUP_574171), String.class);
        assertThat(resolved, is(BENGALURU_CAL));

        // Members at the same address resolve the same way.
        assertThat(jdbcTemplate.queryForObject(format("select calendar_uuid from orgc.subject_resolved_calendar where subject_uuid = '%s'", PERSON_574170), String.class), is(BENGALURU_CAL));

        // A subject outside any calendar subtree, with no org default, is excluded (no row).
        assertThat(jdbcTemplate.queryForList(format("select 1 from orgc.subject_resolved_calendar where subject_uuid = '%s'", GROUP_574180_UNRESOLVED)).size(), is(0));
    }

    @Test
    @Sql({"/attendance-test-data-teardown.sql", "/test-data-teardown.sql", "/test-data.sql", "/attendance-test-data.sql"})
    @Sql(scripts = {"/attendance-test-data-teardown.sql", "/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void expectedSessionsAppliesCalendarInclusionAndStatus() {
        runEtl();

        // Held session on a working day.
        Map<String, Object> mon = expectedSession("2024-03-04");
        assertThat(mon.get("calendar_day_type"), is("working_day"));
        assertThat(mon.get("status"), is("Held"));

        // Working day with no session -> Unmarked.
        Map<String, Object> wed = expectedSession("2024-03-06");
        assertThat(wed.get("calendar_day_type"), is("working_day"));
        assertThat(wed.get("status"), is("Unmarked"));

        // Sunday working-override is included.
        assertThat(expectedSession("2024-03-10").get("calendar_day_type"), is("working_override"));

        // Public holiday is excluded entirely.
        assertThat(expectedSessionRows("2024-03-11").size(), is(0));

        // weekly_off day is excluded from the calendar-driven rows, but a session marked anyway
        // is appended and flagged so reports can exclude it from the default denominator.
        Map<String, Object> markAnyway = expectedSession("2024-03-09");
        assertThat(markAnyway.get("calendar_day_type"), is("mark_anyway"));
        assertThat(markAnyway.get("status"), is("Held"));

        // avniproject/avni-server#1035: the server no longer rejects a Held session on a calendar-off
        // day that carries no reason, so reports identify them here instead — mark_anyway + Held +
        // null reason is exactly the shape that used to deadlock a device's sync queue (FD-8271).
        assertThat(markAnyway.get("reason_concept_uuid"), is(nullValue()));

        // A session that does carry a reason projects the concept uuid, proving the column is a real
        // passthrough rather than always null.
        assertThat(expectedSession("2024-03-16").get("reason_concept_uuid"), is(WHITE_CONCEPT));

        // mark_anyway must not leak sessions of an unresolved-calendar group (group is excluded entirely).
        assertThat(jdbcTemplate.queryForList(format("select 1 from orgc.expected_sessions where group_subject_uuid = '%s'", GROUP_574180_UNRESOLVED)).size(), is(0));

        // mark_anyway must not leak a session recorded against a voided/unconfigured attendance type.
        assertThat(jdbcTemplate.queryForList(format("select 1 from orgc.expected_sessions where attendance_type_uuid = '%s'", EVENING_TYPE_VOIDED)).size(), is(0));

        // A second group subject type (Class, type Group) is iterated alongside Household via UNION ALL:
        // its Held session on a working day appears in expected_sessions.
        Map<String, Object> classSession = jdbcTemplate.queryForMap(format(
                "select calendar_day_type, status from orgc.expected_sessions where group_subject_uuid='%s' and attendance_type_uuid='%s' and scheduled_date='2024-03-04'",
                CLASS_GROUP_574190, CLASS_TYPE));
        assertThat(classSession.get("calendar_day_type"), is("working_day"));
        assertThat(classSession.get("status"), is("Held"));
    }

    @Test
    @Sql({"/attendance-test-data-teardown.sql", "/test-data-teardown.sql", "/test-data.sql", "/attendance-test-data.sql"})
    @Sql(scripts = {"/attendance-test-data-teardown.sql", "/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void perStudentAttendanceComputesStatusAndRespectsMembership() {
        runEtl();

        // 2024-03-04 session is Held with explicit records.
        assertThat(studentStatus(PERSON_574170, "2024-03-04"), is("Present"));
        assertThat(studentStatus(PERSON_574173, "2024-03-04"), is("Absent"));

        // 2024-03-16 session is Held but has no records -> implied absent for active members.
        assertThat(studentStatus(PERSON_574170, "2024-03-16"), is("Absent_implied"));
        assertThat(studentStatus(PERSON_574173, "2024-03-16"), is("Absent_implied"));

        // 574174 left the group in February -> excluded from March sessions.
        assertThat(studentRows(PERSON_574174, "2024-03-04").size(), is(0));

        // No session on a working day -> no per-student row at all.
        assertThat(jdbcTemplate.queryForList("select 1 from orgc.per_student_attendance where scheduled_date = '2024-03-06'").size(), is(0));
    }

    @Test
    @Sql({"/attendance-test-data-teardown.sql", "/test-data-teardown.sql", "/test-data.sql", "/attendance-test-data.sql"})
    @Sql(scripts = {"/attendance-test-data-teardown.sql", "/test-data-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void viewsAreRebuiltCleanlyOnRerun() {
        // The materialized views (and the subject tables expected_sessions reads) must drop and
        // recreate cleanly across consecutive syncs without dependency errors.
        runEtl();
        runEtl();
        assertThat(countOfRowsIn("orgc.expected_sessions"), greaterThan(0L));
        assertThat(countOfRowsIn("orgc.per_student_attendance"), greaterThan(0L));
    }

    private Map<String, Object> expectedSession(String date) {
        return jdbcTemplate.queryForMap(format(
                "select calendar_day_type, status, reason_concept_uuid from orgc.expected_sessions where group_subject_uuid='%s' and attendance_type_uuid='%s' and scheduled_date='%s'",
                GROUP_574171, MORNING_TYPE, date));
    }

    private List<Map<String, Object>> expectedSessionRows(String date) {
        return jdbcTemplate.queryForList(format(
                "select 1 from orgc.expected_sessions where group_subject_uuid='%s' and attendance_type_uuid='%s' and scheduled_date='%s'",
                GROUP_574171, MORNING_TYPE, date));
    }

    private String studentStatus(String studentUuid, String date) {
        return jdbcTemplate.queryForObject(format(
                "select status from orgc.per_student_attendance where student_subject_uuid='%s' and scheduled_date='%s'", studentUuid, date), String.class);
    }

    private List<Map<String, Object>> studentRows(String studentUuid, String date) {
        return jdbcTemplate.queryForList(format(
                "select 1 from orgc.per_student_attendance where student_subject_uuid='%s' and scheduled_date='%s'", studentUuid, date));
    }
}
