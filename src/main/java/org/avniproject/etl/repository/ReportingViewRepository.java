package org.avniproject.etl.repository;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.ReportingViewMetaData;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.util.List;

import static org.avniproject.etl.repository.sql.SqlFile.readFile;

@Repository
public class ReportingViewRepository  implements ReportingViewMetaData{
    private static final Logger log = Logger.getLogger(ReportingViewRepository.class);
    private final JdbcTemplate jdbcTemplate;
    private final String individualViewFile = readFile("/sql/etl/view/individualAddressView.sql.st");
    private final String enrolmentViewFile = readFile("/sql/etl/view/enrolmentAddressView.sql.st");
    private final String grantViewFile = readFile("/sql/etl/view/grantView.sql.st");

    public ReportingViewRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void createOrReplaceView(OrganisationIdentity organisationIdentity){
        String schemaName = organisationIdentity.getSchemaName();
        List<String> usersWithSchemaAccess = organisationIdentity.getUsersWithSchemaAccess();
        createViewAndGrantPermission(Type.INDIVIDUAL, individualViewFile,schemaName,usersWithSchemaAccess);
        createViewAndGrantPermission(Type.ENROLMENT, enrolmentViewFile,schemaName,usersWithSchemaAccess);
    }

    private void createViewAndGrantPermission(Type type,String viewFileName,String schemaName,List<String> users){
        String viewName = getViewName(type);
        ST st = new ST(viewFileName);
        st.add(SCHEMA_NAME,schemaName);
        st.add(VIEW_NAME,viewName);
        String query = st.render();
        try {
            jdbcTemplate.execute(query);
            log.info(String.format("%s view created",viewName));
            users.forEach(user->grantPermissionToView(schemaName,viewName,user));
        }catch(DataAccessException  exception){
            log.debug(String.format("Unable to create view %s",viewName),exception);
        }
    }


    private void grantPermissionToView(String schemaName,String viewName,String userName){
        ST st = new ST(grantViewFile);
        st.add(SCHEMA_NAME,schemaName);
        st.add(VIEW_NAME,viewName);
        st.add(USER_NAME,userName);
        String query = st.render();
        try {
            jdbcTemplate.execute(query);
        }catch (DataAccessException exception){
            log.debug(String.format("Unable to grant permission of %s to %s",viewName,userName),exception);
        }
    }
}
