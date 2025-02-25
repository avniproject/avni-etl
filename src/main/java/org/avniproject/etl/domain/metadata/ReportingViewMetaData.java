package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.OrganisationIdentity;

public interface ReportingViewMetaData {

    String INDIVIDUAL_VIEW_NAME = "individual_address_view";
    String ENROLMENT_VIEW_NAME = "enrolment_address_view";

    String SCHEMA_NAME = "schema_name";
    String VIEW_NAME = "view_name";
    String USER_NAME = "user_name";

    enum Type{
        INDIVIDUAL,
        ENROLMENT
    }

    default String getViewName(Type type){
        return switch (type) {
            case INDIVIDUAL -> INDIVIDUAL_VIEW_NAME;
            case ENROLMENT -> ENROLMENT_VIEW_NAME;
        };

    }


    void createOrReplaceView(OrganisationIdentity organisationIdentity);

}
