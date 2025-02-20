package org.avniproject.etl.repository;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.ReportingViewMetaData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ReportingViewRepository implements ReportingViewMetaData {
    private static final Logger log = Logger.getLogger(ReportingViewRepository.class);
    private final JdbcTemplate jdbcTemplate;

    public ReportingViewRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void createOrReplaceView(OrganisationIdentity organisationIdentity){
        String schemaName = organisationIdentity.getSchemaName();
        List<String> usersWithSchemaAccess = organisationIdentity.getUsersWithSchemaAccess();
        createView(schemaName,usersWithSchemaAccess,Type.INDIVIDUAL);
        createView(schemaName,usersWithSchemaAccess,Type.ENROLMENT);
    }


    private void createView(String schemaName, List<String> usersWithSchemaAccess,Type type){
        String viewName = getViewName(schemaName,type);
        String query = generateQuery(viewName,schemaName,type);
        try {
            jdbcTemplate.execute(query);
            log.info(String.format("%s view created",viewName));
            usersWithSchemaAccess.stream().forEach(user->grantPermissionToView(viewName,user));
        }
        catch (Exception exception){
            //SQL Exception Handle
            log.debug(String.format("Unable to Create View %s",viewName),exception);
        }
    }



    private void grantPermissionToView(String viewName,String userName){
        String query = getGrantQuery(viewName, userName);
        try{
            jdbcTemplate.execute(query);
        }catch (Exception exception){
            //SQL Exception Handle
            log.info(String.format("Unable to grant permission of %s to %s",viewName,userName),exception);
        }
    }

}
