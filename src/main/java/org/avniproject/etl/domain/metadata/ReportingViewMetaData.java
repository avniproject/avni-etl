package org.avniproject.etl.domain.metadata;

public interface ReportingViewMetaData {
    String INDIVIDUAL_VIEW_QUERY = """
                            CREATE OR REPLACE VIEW %s AS
                            SELECT
                                ind.id AS individual_id,
                                st.name AS subject_type,
                                ind.uuid AS individual_uuid,
                                ind.first_name,
                                ind.last_name,
                                ind.middle_name,
                                ind.date_of_birth,
                                g.name AS gender,
                                ind.registration_date AS individual_registration_date,
                                ind.created_date_time AS individual_created_date_time,
                                ind.last_modified_date_time As individual_last_modified_date_time,
                                address.*
                            FROM public.individual ind
                                     JOIN public.subject_type st ON ind.subject_type_id = st.id
                                     JOIN %s.address ON ind.address_id = address.id
                                     LEFT JOIN public.gender g ON ind.gender_id = g.id;
            """;

    String ENROLMENT_VIEW_QUERY = """
                            CREATE OR REPLACE VIEW %s AS
                            SELECT
                                pe.id AS enrolment_id,
                                pe.uuid AS enrolment_uuid,
                                p.name AS program_name,
                                ind.id AS individual_id,
                                ind.uuid AS individual_uuid,
                                ind.first_name,
                                ind.last_name,
                                pe.enrolment_date_time,
                                pe.created_date_time AS enrolment_created_date_time,
                                pe.last_modified_date_time AS enrolment_last_modified_date_time
                            FROM public.individual ind
                            JOIN public.program_enrolment pe ON ind.id = pe.individual_id
                            JOIN public.program p ON pe.program_id = p.id
                            JOIN %s.address ON ind.address_id = address.id;
            """;
    String GRANT_QUERY = """
                             GRANT ALL PRIVILEGES ON %s TO %s;
            """;

    String INDIVIDUAL_VIEW_POSTFIX = "individual_address_view";
    String ENROLMENT_VIEW_POSTFIX = "enrolment_address_view";

    enum Type{
        INDIVIDUAL,
        ENROLMENT
    }

    default String generateQuery(String viewName,String schemaName,Type type){
        return switch(type){
            case INDIVIDUAL -> String.format(INDIVIDUAL_VIEW_QUERY,viewName,schemaName);
            case ENROLMENT -> String.format(ENROLMENT_VIEW_QUERY,viewName,schemaName);
            default -> throw new IllegalArgumentException(String.format("Unable to find %s definition",type.name()));
        };
    }

    default String getViewName(String schemaName, Type type){
        String viewString = "%s_%s";
        return switch(type){
            case INDIVIDUAL -> String.format(viewString,schemaName, INDIVIDUAL_VIEW_POSTFIX);
            case ENROLMENT -> String.format(viewString,schemaName, ENROLMENT_VIEW_POSTFIX);
            default -> throw new IllegalArgumentException(String.format("Unable to find %s definition",type.name()));
        };

    }

    default String getGrantQuery(String viewName, String user){
        return String.format(GRANT_QUERY,viewName,user);
    }

}
