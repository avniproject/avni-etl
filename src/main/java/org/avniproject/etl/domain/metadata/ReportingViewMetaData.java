package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.OrganisationIdentity;

public interface ReportingViewMetaData {

    String SCHEMA_NAME = "schema_name";
    String VIEW_NAME = "view_name";
    String USER_NAME = "user_name";

    String SCHEMA_NAME = "schema_name";
    String POST_FIX = "postfix";
    String VIEW_NAME = "view_name";
    String USER_NAME = "user_name";

    enum Type{
        INDIVIDUAL,
        ENROLMENT
    }

    default String getViewName(String schemaName, Type type){
        return String.format("%s_%s", schemaName, getPostfix(type));
    }

    default String getPostfix(Type type) {
        return switch (type) {
            case INDIVIDUAL -> INDIVIDUAL_VIEW_POSTFIX;
            case ENROLMENT -> ENROLMENT_VIEW_POSTFIX;
        };
    }

    void createOrReplaceView(OrganisationIdentity organisationIdentity);

}
