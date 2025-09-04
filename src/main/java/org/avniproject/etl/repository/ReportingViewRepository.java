package org.avniproject.etl.repository;

import jakarta.annotation.PostConstruct;
import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.ReportingViewMetaData;
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

    private final Map<Type, ViewConfig> viewConfigs = new HashMap<>();
    private final OrganisationRepository organisationRepository;
    private final TableMetadataRepository tableMetadataRepository;

    public enum Type {
        SUBJECT, ENROLMENT, DUE_VISITS, COMPLETED_VISITS, OVERDUE_VISITS
    }

    public ReportingViewRepository(JdbcTemplate jdbcTemplate, OrganisationRepository organisationRepository, TableMetadataRepository tableMetadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.organisationRepository = organisationRepository;
        this.tableMetadataRepository = tableMetadataRepository;
    }

    @PostConstruct
    public void init() {
        viewConfigs.put(Type.SUBJECT, new ViewConfig("subject_view",
                "and st.organisation_id in (%s)", "", subjectViewFile));
        viewConfigs.put(Type.ENROLMENT, new ViewConfig("enrolment_view",
                "and p.organisation_id in (%s)", "", enrolmentViewFile));
        viewConfigs.put(Type.DUE_VISITS, new ViewConfig("due_visits_view",
                "WHERE t.earliest_visit_date_time < CURRENT_DATE AND CURRENT_DATE < t.max_visit_date_time AND t.is_voided IS false",
                "", baseVisitsViewFile));
        viewConfigs.put(Type.COMPLETED_VISITS, new ViewConfig("completed_visits_view",
                "WHERE t.encounter_date_time IS NOT NULL AND t.cancel_date_time IS NULL AND t.is_voided IS false",
                "", baseVisitsViewFile));
        viewConfigs.put(Type.OVERDUE_VISITS, new ViewConfig("overdue_visits_view",
                "WHERE CURRENT_DATE > t.max_visit_date_time AND t.encounter_date_time is NULL AND t.cancel_date_time is NULL AND t.is_voided IS false",
                "", baseVisitsViewFile));
    }

    @Override
    public void createOrReplaceView(OrganisationIdentity organisationIdentity) {
        String schemaName = organisationIdentity.getSchemaName();
        List<String> addressColumns = getAddressColumnNames(organisationIdentity);
        List<String> usersWithSchemaAccess = organisationIdentity.getUsersWithSchemaAccess();
        for (Type type : Type.values()) {
            ViewConfig config = viewConfigs.get(type);
            createViewAndGrantPermission(type, config, schemaName, usersWithSchemaAccess, addressColumns, organisationIdentity);
        }
    }

    private List<String> getAddressColumnNames(OrganisationIdentity organisationIdentity) {
        ST st = new ST(addressLevelTypeNamesFile);
        st.add(SCHEMA_PARAM_NAME, organisationIdentity.getSchemaName());
        st.add(DB_USER, organisationIdentity.getDbUser());
        String query = st.render();
        return jdbcTemplate.queryForList(query, String.class);
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

    private void createViewAndGrantPermission(Type type, ViewConfig config, String schemaName, List<String> users, List<String> addressColumns, OrganisationIdentity organisationIdentity) {
        List<TableMetadataST> tableMetadata = switch (type) {
            case SUBJECT ->
                    tableMetadataRepository.fetchByType(List.of(TableMetadata.Type.Individual, TableMetadata.Type.Person, TableMetadata.Type.Household, TableMetadata.Type.Group));
            case ENROLMENT -> tableMetadataRepository.fetchByType(List.of(TableMetadata.Type.ProgramEnrolment));
            case DUE_VISITS, COMPLETED_VISITS, OVERDUE_VISITS ->
                    tableMetadataRepository.fetchByType(List.of(TableMetadata.Type.Encounter, TableMetadata.Type.IndividualEncounterCancellation, TableMetadata.Type.ProgramEncounter, TableMetadata.Type.ProgramEncounterCancellation));
        };
        ST st = new ST(config.getSqlTemplateFile());
        st.add(SCHEMA_PARAM_NAME, schemaName);
        st.add(VIEW_PARAM_NAME, config.getViewName());
        st.add(ADDRESS_COLUMNS_PARAM_NAME, addressColumns);
        st.add(EXTRA_COLUMNS, config.getExtraColumns());
        st.add(TABLE_METADATA, tableMetadata);

        String whereClause = config.getWhereClause();
        List<Long> organisationIds = organisationRepository.getOrganisationIds(organisationIdentity);
        String organisationIdsString = organisationIds.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
        st.add(WHERE_CLAUSE, String.format(whereClause, organisationIdsString));

        String query = st.render();

        executeQueryInContext(organisationIdentity, query, "created", config.getViewName(), schemaName);
        users.forEach(user -> grantPermissionToView(schemaName, config.getViewName(), user));
    }

    public void grantPermissionToView(String schemaName, String viewName, String userName) {
        ST st = new ST(grantViewFile);
        st.add(SCHEMA_PARAM_NAME, schemaName);
        st.add(VIEW_PARAM_NAME, viewName);
        st.add(USER_PARAM_NAME, userName);
        String query = st.render();
        executeQueryInContext(OrgIdentityContextHolder.getOrganisationIdentity(), query, "granted permission to", viewName, schemaName);
    }
}
