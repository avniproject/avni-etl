package org.avniproject.etl.repository;

import jakarta.annotation.PostConstruct;
import org.apache.log4j.Logger;
import org.avniproject.etl.config.EtlServiceConfig;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.ReportingViewMetaData;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.dto.TableMetadataST;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;
import static org.avniproject.etl.repository.JdbcContextWrapper.runInSchemaUserContext;
import static org.avniproject.etl.repository.sql.SqlFile.readFile;

@Repository
public class ReportingViewRepository implements ReportingViewMetaData {
    private static final Logger log = Logger.getLogger(ReportingViewRepository.class);
    private final JdbcTemplate jdbcTemplate;
    private final String addressLevelTypeNamesFile = readFile("/sql/etl/view/addressLevelTypeNames.sql.st");
    private final String subjectViewFile = readFile("/sql/etl/view/subjectView.sql.st");
    private final String enrolmentViewFile = readFile("/sql/etl/view/enrolmentView.sql.st");
    private final String baseVisitsViewFile = readFile("/sql/etl/view/baseVisitsView.sql.st");
    private final String grantViewFile = readFile("/sql/etl/view/grantView.sql.st");
    private final String workingDayCalendarFile = readFile("/sql/etl/view/workingDayCalendar.sql.st");
    private final String subjectResolvedCalendarFile = readFile("/sql/etl/view/subjectResolvedCalendar.sql.st");
    private final String expectedSessionsFile = readFile("/sql/etl/view/expectedSessions.sql.st");
    private final String perStudentAttendanceFile = readFile("/sql/etl/view/perStudentAttendance.sql.st");

    private final Map<Type, ViewConfig> viewConfigs = new HashMap<>();
    private final OrganisationRepository organisationRepository;
    private final EtlServiceConfig etlServiceConfig;

    // Declaration order also drives creation order via Type.values(): the materialized views
    // must be built after their dependencies (working_day_calendar + subject_resolved_calendar
    // before expected_sessions before per_student_attendance).
    public enum Type {
        SUBJECT, ENROLMENT, DUE_VISITS, COMPLETED_VISITS, OVERDUE_VISITS,
        WORKING_DAY_CALENDAR, SUBJECT_RESOLVED_CALENDAR, EXPECTED_SESSIONS, PER_STUDENT_ATTENDANCE
    }

    public ReportingViewRepository(JdbcTemplate jdbcTemplate, OrganisationRepository organisationRepository,
                                   EtlServiceConfig etlServiceConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.organisationRepository = organisationRepository;
        this.etlServiceConfig = etlServiceConfig;
    }

    @PostConstruct
    public void init() {
        viewConfigs.put(Type.SUBJECT, new ViewConfig("subject_view",
                "and st.organisation_id in (%s)", "", subjectViewFile,
                List.of(TableMetadata.Type.Individual, TableMetadata.Type.Person, TableMetadata.Type.Household, TableMetadata.Type.Group)));
        viewConfigs.put(Type.ENROLMENT, new ViewConfig("enrolment_view",
                "and p.organisation_id in (%s)", "", enrolmentViewFile,
                List.of(TableMetadata.Type.ProgramEnrolment)));
        viewConfigs.put(Type.DUE_VISITS, new ViewConfig("due_visits_view",
                "WHERE t.earliest_visit_date_time < CURRENT_DATE AND CURRENT_DATE < t.max_visit_date_time AND t.is_voided IS false",
                "", baseVisitsViewFile, VISIT_METADATA_TYPES));
        viewConfigs.put(Type.COMPLETED_VISITS, new ViewConfig("completed_visits_view",
                "WHERE t.encounter_date_time IS NOT NULL AND t.cancel_date_time IS NULL AND t.is_voided IS false",
                "", baseVisitsViewFile, VISIT_METADATA_TYPES));
        viewConfigs.put(Type.OVERDUE_VISITS, new ViewConfig("overdue_visits_view",
                "WHERE CURRENT_DATE > t.max_visit_date_time AND t.encounter_date_time is NULL AND t.cancel_date_time is NULL AND t.is_voided IS false",
                "", baseVisitsViewFile, VISIT_METADATA_TYPES));

        // Materialized views (attendance/calendar) — gated to schemas that have at least one
        // non-voided calendar. No org filter needed: they run under the org/schema role, so RLS
        // scopes reads of public tables (same as the passthrough sync templates).
        viewConfigs.put(Type.WORKING_DAY_CALENDAR, new ViewConfig("working_day_calendar",
                "", "", workingDayCalendarFile, List.of(), true, true));
        viewConfigs.put(Type.SUBJECT_RESOLVED_CALENDAR, new ViewConfig("subject_resolved_calendar",
                "", "", subjectResolvedCalendarFile, List.of(), true, true));
        viewConfigs.put(Type.EXPECTED_SESSIONS, new ViewConfig("expected_sessions",
                "", "", expectedSessionsFile,
                List.of(TableMetadata.Type.Group, TableMetadata.Type.Household), true, true));
        viewConfigs.put(Type.PER_STUDENT_ATTENDANCE, new ViewConfig("per_student_attendance",
                "", "", perStudentAttendanceFile, List.of(), true, true));
    }

    private static final List<TableMetadata.Type> VISIT_METADATA_TYPES = List.of(
            TableMetadata.Type.Encounter, TableMetadata.Type.IndividualEncounterCancellation,
            TableMetadata.Type.ProgramEncounter, TableMetadata.Type.ProgramEncounterCancellation);

    @Override
    public void createOrReplaceView(OrganisationIdentity organisationIdentity, SchemaMetadata schemaMetadata) {
        String schemaName = organisationIdentity.getSchemaName();
        List<String> addressColumns = getAddressColumnNames(organisationIdentity);
        List<String> usersWithSchemaAccess = organisationIdentity.getUsersWithSchemaAccess();
        boolean hasCalendar = hasNonVoidedCalendar(organisationIdentity, schemaName);
        for (Type type : Type.values()) {
            ViewConfig config = viewConfigs.get(type);
            if (config.isGated() && !hasCalendar) {
                // Attendance not in use for this org: drop any stale materialized view and skip.
                dropMaterializedViewIfExists(organisationIdentity, schemaName, config.getViewName());
                continue;
            }
            createViewAndGrantPermission(config, schemaName, usersWithSchemaAccess, addressColumns, organisationIdentity, schemaMetadata);
        }
    }

    private boolean hasNonVoidedCalendar(OrganisationIdentity organisationIdentity, String schemaName) {
        String query = String.format("select exists(select 1 from \"%s\".calendar where is_voided = false)", schemaName);
        if (isOrganizationGroupSchema(organisationIdentity)) {
            return Boolean.TRUE.equals(runInSchemaUserContext(() -> jdbcTemplate.queryForObject(query, Boolean.class), jdbcTemplate));
        }
        return Boolean.TRUE.equals(runInOrgContext(() -> jdbcTemplate.queryForObject(query, Boolean.class), jdbcTemplate));
    }

    private void dropMaterializedViewIfExists(OrganisationIdentity organisationIdentity, String schemaName, String viewName) {
        String query = String.format("DROP MATERIALIZED VIEW IF EXISTS \"%s\".\"%s\" CASCADE", schemaName, viewName);
        executeQueryInContext(organisationIdentity, query, "dropped", viewName, schemaName);
    }

    private List<String> getAddressColumnNames(OrganisationIdentity organisationIdentity) {
        ST st = new ST(addressLevelTypeNamesFile);
        st.add(SCHEMA_PARAM_NAME, organisationIdentity.getSchemaName());
        st.add(DB_USER, organisationIdentity.getDbUser());
        String query = st.render();
        
        if (isOrganizationGroupSchema(organisationIdentity)) {
            return runInSchemaUserContext(() -> jdbcTemplate.queryForList(query, String.class), jdbcTemplate);
        } else {
            return runInOrgContext(() -> jdbcTemplate.queryForList(query, String.class), jdbcTemplate);
        }
    }

    private boolean isOrganizationGroupSchema(OrganisationIdentity organisationIdentity) {
        return !organisationIdentity.getSchemaName().equals(organisationIdentity.getDbUser());
    }

    private void executeQueryInContext(OrganisationIdentity organisationIdentity, String query, String operation, String viewName, String schemaName) {
        try {
            if (isOrganizationGroupSchema(organisationIdentity)) {
                runInSchemaUserContext(() -> {
                    jdbcTemplate.execute(query);
                    return null;
                }, jdbcTemplate);
            } else {
                runInOrgContext(() -> {
                    jdbcTemplate.execute(query);
                    return null;
                }, jdbcTemplate);
            }
            log.info(String.format("%s view %s successfully", viewName, operation));
        } catch (Exception e) {
            log.error(String.format("Failed to %s view %s for schema %s. Error: %s",
                    operation, viewName, schemaName, e.getMessage()), e);
            throw e;
        }
    }

    private List<TableMetadataST> filterTableMetadata(SchemaMetadata schemaMetadata, List<TableMetadata.Type> types) {
        return schemaMetadata.getTableMetadata().stream()
                .filter(t -> types.contains(t.getType()))
                .map(t -> new TableMetadataST(
                        t.getName(),
                        t.getType().name(),
                        t.getSubjectTypeUuid(),
                        t.getProgramUuid(),
                        t.getEncounterTypeUuid(),
                        t.getType() == TableMetadata.Type.Person))
                .collect(Collectors.toList());
    }

    private void createViewAndGrantPermission(ViewConfig config, String schemaName, List<String> users, List<String> addressColumns, OrganisationIdentity organisationIdentity, SchemaMetadata schemaMetadata) {
        List<TableMetadataST> tableMetadata = filterTableMetadata(schemaMetadata, config.getMetadataTypes());
        ST st = new ST(config.getSqlTemplateFile());
        st.add(SCHEMA_PARAM_NAME, schemaName);
        st.add(VIEW_PARAM_NAME, config.getViewName());
        st.add(ADDRESS_COLUMNS_PARAM_NAME, addressColumns);
        st.add(EXTRA_COLUMNS, config.getExtraColumns());
        st.add(TABLE_METADATA, tableMetadata);

        List<Long> organisationIds = organisationRepository.getOrganisationIds(organisationIdentity);
        String organisationIdsString = organisationIds.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
        st.add(WHERE_CLAUSE, String.format(config.getWhereClause(), organisationIdsString));
        st.add(STUDENT_WINDOW_MONTHS, etlServiceConfig.getPerStudentAttendanceWindowInMonths());

        String query = st.render();

        executeQueryInContext(organisationIdentity, query, "created", config.getViewName(), schemaName);
        log.info(String.format("%s view created", config.getViewName()));
        users.forEach(user -> grantPermissionToView(schemaName, config.getViewName(), user));
    }

    public void grantPermissionToView(String schemaName, String viewName, String userName) {
        ST st = new ST(grantViewFile);
        st.add(SCHEMA_PARAM_NAME, schemaName);
        st.add(VIEW_PARAM_NAME, viewName);
        st.add(USER_PARAM_NAME, userName);
        String query = st.render();
        jdbcTemplate.execute(query);
    }
}
