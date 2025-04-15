package org.avniproject.etl.repository;

import jakarta.annotation.PostConstruct;
import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.ReportingViewMetaData;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.avniproject.etl.repository.sql.SqlFile.readFile;

@Repository
public class ReportingViewRepository implements ReportingViewMetaData {
    private static final String ALL_ADDRESS_COLUMNS = "address.*";
    private static final Logger log = Logger.getLogger(ReportingViewRepository.class);
    private final JdbcTemplate jdbcTemplate;
    private final String addressLevelTypeNamesFile = readFile("/sql/etl/view/addressLevelTypeNames.sql.st");
    private final String subjectViewFile = readFile("/sql/etl/view/subjectView.sql.st");
    private final String enrolmentViewFile = readFile("/sql/etl/view/enrolmentView.sql.st");
    private final String baseVisitsViewFile = readFile("/sql/etl/view/baseVisitsView.sql.st");
    private final String grantViewFile = readFile("/sql/etl/view/grantView.sql.st");

    private final Map<Type, ViewConfig> viewConfigs = new HashMap<>();

    public enum Type {
        SUBJECT, ENROLMENT, DUE_VISITS, COMPLETED_VISITS, OVERDUE_VISITS
    }

    public ReportingViewRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void init() {
        viewConfigs.put(Type.SUBJECT, new ViewConfig("subject_view", "", "", subjectViewFile));
        viewConfigs.put(Type.ENROLMENT, new ViewConfig("enrolment_view", "", "", enrolmentViewFile));
        viewConfigs.put(Type.DUE_VISITS, new ViewConfig("due_visits_view",
                "WHERE e.earliest_visit_date_time < CURRENT_DATE AND CURRENT_DATE < e.max_visit_date_time",
                "", baseVisitsViewFile));
        viewConfigs.put(Type.COMPLETED_VISITS, new ViewConfig("completed_visits_view",
                "WHERE e.encounter_date_time IS NOT NULL AND e.cancel_date_time IS NULL",
                "e.encounter_date_time,", baseVisitsViewFile));
        viewConfigs.put(Type.OVERDUE_VISITS, new ViewConfig("overdue_visits_view",
                "WHERE CURRENT_DATE > e.max_visit_date_time",
                "", baseVisitsViewFile));
    }

    @Override
    public void createOrReplaceView(OrganisationIdentity organisationIdentity) {
        String schemaName = organisationIdentity.getSchemaName();
        String addressColumns = getAddressColumnNames(organisationIdentity);
        List<String> usersWithSchemaAccess = organisationIdentity.getUsersWithSchemaAccess();
        for (Type type : Type.values()) {
            ViewConfig config = viewConfigs.get(type);
            createViewAndGrantPermission(config, schemaName, usersWithSchemaAccess, addressColumns);
        }
    }

    private String getAddressColumnNames(OrganisationIdentity organisationIdentity) {
        ST st = new ST(addressLevelTypeNamesFile);
        st.add(SCHEMA_PARAM_NAME, organisationIdentity.getSchemaName());
        String query = st.render();
        try {
            return jdbcTemplate.queryForObject(query, String.class);
        } catch (DataAccessException exception) {
            log.debug("Unable to fetch addressLevelType names", exception);
        }
        return ALL_ADDRESS_COLUMNS;
    }

    private void createViewAndGrantPermission(ViewConfig config, String schemaName, List<String> users, String addressColumns) {
        ST st = new ST(config.getSqlTemplateFile());
        st.add(SCHEMA_PARAM_NAME, schemaName);
        st.add(VIEW_PARAM_NAME, config.getViewName());
        st.add(ADDRESS_COLUMNS_PARAM_NAME, addressColumns);
        st.add(WHERE_CLAUSE, config.getWhereClause());
        st.add(EXTRA_COLUMNS, config.getExtraColumns());

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
