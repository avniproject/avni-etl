package org.avniproject.etl.repository;

import jakarta.annotation.PostConstruct;
import org.apache.log4j.Logger;
import org.assertj.core.util.Strings;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.ReportingViewMetaData;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    public enum Type {
        SUBJECT, ENROLMENT, DUE_VISITS, COMPLETED_VISITS, OVERDUE_VISITS
    }

    public ReportingViewRepository(JdbcTemplate jdbcTemplate, OrganisationRepository organisationRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.organisationRepository = organisationRepository;
    }

    @PostConstruct
    public void init() {
        viewConfigs.put(Type.SUBJECT, new ViewConfig("subject_view",
                "where ind.organisation_id in (%s)", "", subjectViewFile));
        viewConfigs.put(Type.ENROLMENT, new ViewConfig("enrolment_view",
                "where pe.organisation_id in (%s)", "", enrolmentViewFile));
        viewConfigs.put(Type.DUE_VISITS, new ViewConfig("due_visits_view",
                "WHERE e.organisation_id in (%s) AND e.earliest_visit_date_time < CURRENT_DATE AND CURRENT_DATE < e.max_visit_date_time",
                "", baseVisitsViewFile));
        viewConfigs.put(Type.COMPLETED_VISITS, new ViewConfig("completed_visits_view",
                "WHERE e.organisation_id in (%s) AND e.encounter_date_time IS NOT NULL AND e.cancel_date_time IS NULL",
                "", baseVisitsViewFile));
        viewConfigs.put(Type.OVERDUE_VISITS, new ViewConfig("overdue_visits_view",
                "WHERE e.organisation_id in (%s) AND CURRENT_DATE > e.max_visit_date_time",
                "", baseVisitsViewFile));
    }

    @Override
    public void createOrReplaceView(OrganisationIdentity organisationIdentity) {
        String schemaName = organisationIdentity.getSchemaName();
        List<String> addressColumns = getAddressColumnNames(organisationIdentity);
        List<String> usersWithSchemaAccess = organisationIdentity.getUsersWithSchemaAccess();
        for (Type type : Type.values()) {
            ViewConfig config = viewConfigs.get(type);
            createViewAndGrantPermission(config, schemaName, usersWithSchemaAccess, addressColumns, organisationIdentity);
        }
    }

    private List<String> getAddressColumnNames(OrganisationIdentity organisationIdentity) {
        ST st = new ST(addressLevelTypeNamesFile);
        st.add(SCHEMA_PARAM_NAME, organisationIdentity.getSchemaName());
        st.add(DB_USER, organisationIdentity.getDbUser());
        String query = st.render();
        return jdbcTemplate.queryForList(query, String.class);
    }

    private void createViewAndGrantPermission(ViewConfig config, String schemaName, List<String> users, List<String> addressColumns, OrganisationIdentity organisationIdentity) {
        ST st = new ST(config.getSqlTemplateFile());
        st.add(SCHEMA_PARAM_NAME, schemaName);
        st.add(VIEW_PARAM_NAME, config.getViewName());
        st.add(ADDRESS_COLUMNS_PARAM_NAME, addressColumns);
        st.add(EXTRA_COLUMNS, config.getExtraColumns());

        String whereClause = config.getWhereClause();
        List<Long> organisationIds = organisationRepository.getOrganisationIds(organisationIdentity);
        String organisationIdsString = organisationIds.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
        st.add(WHERE_CLAUSE, String.format(whereClause, organisationIdsString));

        String query = st.render();
        jdbcTemplate.execute(query);
        log.info(String.format("%s view created", config.getViewName()));
        users.forEach(user -> grantPermissionToView(schemaName, config.getViewName(), user));
    }

    private void grantPermissionToView(String schemaName, String viewName, String userName) {
        ST st = new ST(grantViewFile);
        st.add(SCHEMA_PARAM_NAME, schemaName);
        st.add(VIEW_PARAM_NAME, viewName);
        st.add(USER_PARAM_NAME, userName);
        String query = st.render();
        jdbcTemplate.execute(query);
    }
}
