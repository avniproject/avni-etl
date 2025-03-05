package org.avniproject.etl.domain.metadata;

import org.avniproject.etl.domain.OrganisationIdentity;

public interface ReportingViewMetaData {
    String SUBJECT_VIEW_NAME = "subject_view";
    String ENROLMENT_VIEW_NAME = "enrolment_view";

    String SCHEMA_PARAM_NAME = "schema_name";
    String VIEW_PARAM_NAME = "view_name";
    String ADDRESS_COLUMNS_PARAM_NAME = "address_columns";
    String USER_PARAM_NAME = "user_name";

    enum Type{
        SUBJECT,
        ENROLMENT
    }

    default String getViewName(Type type){
        return switch (type) {
            case SUBJECT -> SUBJECT_VIEW_NAME;
            case ENROLMENT -> ENROLMENT_VIEW_NAME;
        };

    }

    void createOrReplaceView(OrganisationIdentity organisationIdentity);
}
