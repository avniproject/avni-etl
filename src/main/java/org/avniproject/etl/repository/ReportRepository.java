package org.avniproject.etl.repository;

import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.dto.AggregateReportResult;
import org.apache.log4j.Logger;
import org.avniproject.etl.dto.UserActivityDTO;
import org.avniproject.etl.repository.rowMappers.reports.AggregateReportMapper;
import org.avniproject.etl.repository.rowMappers.reports.UserActivityMapper;
import org.avniproject.etl.repository.rowMappers.reports.UserCountMapper;
import org.avniproject.etl.repository.rowMappers.reports.UserDetailsMapper;
import org.avniproject.etl.service.EtlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

@Component
public class ReportRepository {
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final SchemaMetadataRepository schemaMetadataRepository;
    private static final Logger log = Logger.getLogger(EtlService.class);

    @Autowired
    public ReportRepository(NamedParameterJdbcTemplate jdbcTemplate, SchemaMetadataRepository schemaMetadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.schemaMetadataRepository = schemaMetadataRepository;
    }

    public List<UserActivityDTO> generateUserActivity(String orgSchemaName, String subjectWhere, String encounterWhere, String enrolmentWhere, String userWhere) {
        SchemaMetadata schema = schemaMetadataRepository.getExistingSchemaMetadata();
        List<TableMetadata> subjectTables = Stream.of(schema.getAllSubjectTables()).flatMap(Collection::stream).toList();
        List<TableMetadata> encounterTables = schema.getAllEncounterTables().stream().toList();
        List<TableMetadata> programEnrolmentTables = schema.getAllProgramEnrolmentTables().stream().toList();
        List<TableMetadata> programEncounterTables = schema.getAllProgramEncounterTables().stream().toList();

        StringBuilder query = new StringBuilder();

        String baseQuery1 = "select last_modified_by_id, count(*) as registration_count\n" +
                "         from ${schemaName}.${subject}\n" +
                "         where is_voided = false\n" +
                "         ${subjectWhere}\n" +
                "         group by last_modified_by_id\n" ;
        baseQuery1 = baseQuery1.replace("${schemaName}", orgSchemaName)
                .replace("${subjectWhere}", subjectWhere);
        query.append("with registrations as (\n");
        query.append(baseQuery1.replace("${subject}", subjectTables.get(0).getName()));
        for (int i = 1 ; i < subjectTables.size() ; i++) {
            query.append("union all\n" );
            query.append(baseQuery1.replace("${subject}", subjectTables.get(i).getName()));
        }
        query.append("),\n");

        String baseQuery2 = "select last_modified_by_id, count(*) as encounter_count\n" +
                "         from ${schemaName}.${encounter}\n" +
                "         where is_voided = false\n" +
                "         ${encounterWhere}\n" +
                "         group by last_modified_by_id\n" ;
        baseQuery2 = baseQuery2.replace("${schemaName}", orgSchemaName)
                .replace("${encounterWhere}", encounterWhere);
        query.append("encounters as (\n");
        query.append(baseQuery2.replace("${encounter}", encounterTables.get(0).getName()));
        for (int i = 1 ; i < encounterTables.size() ; i++) {
            query.append("union all\n" );
            query.append(baseQuery2.replace("${encounter}", encounterTables.get(i).getName()));
        }
        query.append("),\n");

        String baseQuery3 = "select last_modified_by_id, count(*) as enrolment_count\n" +
                "         from ${schemaName}.${enrolment}\n" +
                "         where is_voided = false\n" +
                "         ${enrolmentWhere}\n" +
                "         group by last_modified_by_id\n" ;
        baseQuery3 = baseQuery3.replace("${schemaName}", orgSchemaName)
                        .replace("${enrolmentWhere}", enrolmentWhere);
        query.append("enrolments as (\n");
        query.append(baseQuery3.replace("${enrolment}", programEnrolmentTables.get(0).getName()));
        for (int i = 1 ; i < programEnrolmentTables.size() ; i++) {
            query.append("union all\n" );
            query.append(baseQuery3.replace("${enrolment}", programEnrolmentTables.get(i).getName()));
        }
        query.append("),\n");

        String baseQuery4 = "select last_modified_by_id, count(*) as program_encounter_count\n" +
                "         from ${schemaName}.${programEncounter}\n" +
                "         where is_voided = false\n" +
                "         ${encounterWhere}\n" +
                "         group by last_modified_by_id\n" ;
        baseQuery4 = baseQuery4.replace("${schemaName}", orgSchemaName)
                .replace("${encounterWhere}", encounterWhere);
        query.append("program_encounters as (\n");
        query.append(baseQuery4.replace("${programEncounter}", programEncounterTables.get(0).getName()));
        for (int i = 1 ; i < programEncounterTables.size() ; i++) {
            query.append("union all\n" );
            query.append(baseQuery4.replace("${programEncounter}", programEncounterTables.get(i).getName()));
        }
        query.append("),\n");

        String baseQuery5 = "activity_table as (\n" +
                "select u.id                                              as id,\n" +
                "       coalesce(u.name, u.username)                      as name,\n" +
                "       coalesce(registration_count, 0)                   as registration_count,\n" +
                "       coalesce(encounter_count, 0)                      as encounter_count,\n" +
                "       coalesce(enrolment_count, 0)                      as enrolment_count,\n" +
                "       coalesce(program_encounter_count, 0)              as program_encounter_count\n" +
                "from ${schemaName}.users u\n" +
                "         left join registrations r on r.last_modified_by_id = u.id\n" +
                "         left join encounters e on e.last_modified_by_id = u.id\n" +
                "         left join enrolments enl on enl.last_modified_by_id = u.id\n" +
                "         left join program_encounters enc on enc.last_modified_by_id = u.id\n" +
                "where (u.is_voided = false or u.is_voided isnull) and u.organisation_id notnull\n" +
                "       and coalesce(coalesce(registration_count, 0) + coalesce(encounter_count, 0) + coalesce(enrolment_count, 0) +\n" +
                "                coalesce(program_encounter_count, 0), 0) > 0\n" +
                "      ${userWhere}\n)," +
                " final_table as (\n" +
                "select id, name,\n"+
                "       sum(distinct registration_count) as registration_count, \n" +
                "       sum(distinct encounter_count) as encounter_count,\n" +
                "       sum(distinct enrolment_count) as enrolment_count,\n" +
                "       sum(distinct program_encounter_count) as program_encounter_count\n" +
                "from activity_table \n" +
                "group by id,name\n" +
                ")" +
                "select *, coalesce(coalesce(registration_count, 0) + coalesce(encounter_count, 0) + coalesce(enrolment_count, 0) +\n" +
                "       coalesce(program_encounter_count, 0), 0) as total\n" +
                "from final_table\n" +
                "order by 7 desc\n" +
                "limit 10;";
        query.append(baseQuery5.replace("${schemaName}", orgSchemaName)
                .replace("${userWhere}", userWhere));

        return jdbcTemplate.query(query.toString(), new UserActivityMapper());
    }

    public List<UserActivityDTO> generateUserSyncFailures(String orgSchemaName, String syncTelemetryWhere, String userWhere) {
        String baseQuery = "select coalesce(u.name, u.username) as name, \n" +
                "       count(*) as count\n" +
                "from ${schemaName}.sync_telemetry st\n" +
                "         join ${schemaName}.users u on st.user_id = u.id\n" +
                "where sync_status = 'incomplete'\n" +
                "and (u.is_voided = false or u.is_voided isnull)\n" +
                "and u.organisation_id notnull\n" +
                "${syncTelemetryWhere}\n"+
                "${userWhere}\n"+
                "group by 1\n" +
                "order by 2 desc\n" +
                "limit 10;";
        String query = baseQuery
                .replace("${schemaName}", orgSchemaName)
                .replace("${syncTelemetryWhere}", syncTelemetryWhere)
                .replace("${userWhere}", userWhere);
        return jdbcTemplate.query(query, new UserCountMapper());
    }

    public List<AggregateReportResult> generateUserAppVersions(String orgSchemaName, String userWhere) {
        String baseQuery = "select app_version as indicator,\n" +
                "       count(*)     as count\n" +
                "from ${schemaName}.users u\n" +
                "         join\n" +
                "     (select user_id,\n" +
                "             app_version,\n" +
                "             row_number() over (partition by user_id order by sync_start_time desc ) as rn\n" +
                "      from ${schemaName}.sync_telemetry) l on l.user_id = u.id and rn = 1\n" +
                "where (u.is_voided = false or u.is_voided isnull) and u.organisation_id notnull\n" +
                "${userWhere}\n"+
                "group by app_version;";
        String query = baseQuery
                .replace("${schemaName}", orgSchemaName)
                .replace("${userWhere}", userWhere);
        return jdbcTemplate.query(query, new AggregateReportMapper());
    }

    public List<AggregateReportResult> generateUserDeviceModels(String orgSchemaName, String userWhere) {
        String baseQuery = "select device_model as indicator,\n" +
                "       count(*)     as count\n" +
                "from ${schemaName}.users u\n" +
                "         join\n" +
                "     (select user_id,\n" +
                "             device_name                          as device_model,\n" +
                "             row_number() over (partition by user_id order by sync_start_time desc ) as rn\n" +
                "      from ${schemaName}.sync_telemetry) l on l.user_id = u.id and rn = 1\n" +
                "where (u.is_voided = false or u.is_voided isnull) and u.organisation_id notnull \n" +
                "${userWhere}\n"+
                "group by device_model;";
        String query = baseQuery
                .replace("${schemaName}", orgSchemaName)
                .replace("${userWhere}", userWhere);
        return jdbcTemplate.query(query, new AggregateReportMapper());
    }

    public List<UserActivityDTO> generateUserDetails(String orgSchemaName, String userWhere) {
        String baseQuery = "select coalesce(u.name, u.username) as name,\n" +
                "       app_version,\n" +
                "       device_model,\n" +
                "       sync_start_time\n" +
                "from ${schemaName}.users u\n" +
                "         join\n" +
                "     (select user_id,\n" +
                "             app_version,\n" +
                "             device_name                          as device_model,\n" +
                "             sync_start_time,\n" +
                "             row_number() over (partition by user_id order by sync_start_time desc ) as rn\n" +
                "      from ${schemaName}.sync_telemetry\n" +
                "      where sync_status = 'complete') l on l.user_id = u.id and rn = 1\n" +
                "where (u.is_voided = false or u.is_voided isnull)\n" +
                "  and u.organisation_id notnull\n" +
                "  ${userWhere}\n"+
                "order by 1 desc;";
        String query = baseQuery
                .replace("${schemaName}", orgSchemaName)
                .replace("${userWhere}", userWhere);
        return jdbcTemplate.query(query, new UserDetailsMapper());
    }

    public List<AggregateReportResult> generateCompletedVisitsOnTimeByProportion(String proportionCondition, String orgSchemaName, String encounterWhere, String userWhere) {
        String baseQuery = "with program_enc_data as (\n" +
                "    select last_modified_by_id,\n" +
                "           count(*) filter ( where encounter_date_time <= max_visit_date_time )                       visits_done_on_time,\n" +
                "           count(*) filter ( where encounter_date_time notnull and earliest_visit_date_time notnull ) total_scheduled\n" +
                "    from program_encounter\n" +
                "    where is_voided = false\n" +
                "    ${encounterWhere}\n" +
                "    group by last_modified_by_id\n" +
                "),\n" +
                "     general_enc_data as (\n" +
                "         select last_modified_by_id,\n" +
                "                count(*) filter ( where encounter_date_time <= max_visit_date_time )              visits_done_on_time,\n" +
                "                count(*)\n" +
                "                filter ( where encounter_date_time notnull and earliest_visit_date_time notnull ) total_scheduled\n" +
                "         from encounter\n" +
                "         where is_voided = false\n" +
                "         ${encounterWhere}\n" +
                "         group by last_modified_by_id\n" +
                "     )\n" +
                "select coalesce(u.name, u.username)                                                as indicator,\n" +
                "       coalesce(ged.visits_done_on_time, 0) + coalesce(ped.visits_done_on_time, 0) as count\n" +
                "from users u\n" +
                "          join general_enc_data ged on ged.last_modified_by_id = u.id\n" +
                "          join program_enc_data ped on ped.last_modified_by_id = u.id\n" +
                "where u.organisation_id notnull\n" +
                "  and is_voided = false\n" +
                "  and coalesce(ged.visits_done_on_time, 0) + coalesce(ped.visits_done_on_time, 0) > 0\n" +
                "  ${userWhere}\n" +
                "  and ((coalesce(ged.visits_done_on_time, 0.0) + coalesce(ped.visits_done_on_time, 0.0)) /\n" +
                "       nullif((coalesce(ged.total_scheduled, 0) + coalesce(ped.total_scheduled, 0)), 0)) ${proportion_condition}\n";
        String query = baseQuery
                .replace("${proportion_condition}", proportionCondition)
                .replace("${schemaName}", orgSchemaName)
                .replace("${encounterWhere}", encounterWhere)
                .replace("${userWhere}", userWhere);
        return jdbcTemplate.query(query, new AggregateReportMapper());
    }

    public List<AggregateReportResult> generateUserCancellingMostVisits(String orgSchemaName, String encounterWhere, String userWhere) {
        String baseQuery = "with program_enc_data as (\n" +
                "    select last_modified_by_id,\n" +
                "           count(*) filter ( where cancel_date_time notnull ) cancelled_visits\n" +
                "    from program_encounter\n" +
                "    where is_voided = false\n" +
                "    ${encounterWhere}\n" +
                "    group by last_modified_by_id\n" +
                "),\n" +
                "     general_enc_data as (\n" +
                "         select last_modified_by_id,\n" +
                "                count(*) filter ( where cancel_date_time notnull ) cancelled_visits\n" +
                "         from encounter\n" +
                "         where is_voided = false\n" +
                "         ${encounterWhere}\n" +
                "         group by last_modified_by_id\n" +
                "     )\n" +
                "select coalesce(u.name, u.username)                                          as indicator,\n" +
                "       coalesce(ged.cancelled_visits, 0) + coalesce(ped.cancelled_visits, 0) as count\n" +
                "from users u\n" +
                "          join general_enc_data ged on ged.last_modified_by_id = u.id\n" +
                "          join program_enc_data ped on ped.last_modified_by_id = u.id\n" +
                "where u.organisation_id notnull\n" +
                "  and is_voided = false\n" +
                "  and coalesce(ged.cancelled_visits, 0) + coalesce(ped.cancelled_visits, 0) > 0 \n" +
                "  ${userWhere}\n" +
                "order by coalesce(ged.cancelled_visits, 0.0) + coalesce(ped.cancelled_visits, 0.0) desc\n" +
                "limit 5;";
        String query = baseQuery
                .replace("${schemaName}", orgSchemaName)
                .replace("${encounterWhere}", encounterWhere)
                .replace("${userWhere}", userWhere);
        return jdbcTemplate.query(query, new AggregateReportMapper());
    }
}