package org.avniproject.etl.repository;

import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.dto.AggregateReportResult;
import org.avniproject.etl.dto.UserActivityDTO;
import org.avniproject.etl.repository.rowMappers.reports.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.stringtemplate.v4.ST;

import java.util.List;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Component
public class ReportRepository {
    private final NamedParameterJdbcTemplate namedJdbcTemplate;
    private final SchemaMetadataRepository schemaMetadataRepository;
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public ReportRepository(NamedParameterJdbcTemplate namedJdbcTemplate, SchemaMetadataRepository schemaMetadataRepository, JdbcTemplate jdbcTemplate) {
        this.namedJdbcTemplate = namedJdbcTemplate;
        this.schemaMetadataRepository = schemaMetadataRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<UserActivityDTO> generateSummaryTable(){
        String baseQuery = "select name, type \n" +
                "from public.table_metadata\n" +
                "where schema_name = '${schemaName}'\n" +
                "order by type;";
        String query= baseQuery.replace("${schemaName}", OrgIdentityContextHolder.getDbSchema());

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new SummaryTableMapper());
    }

    public List<UserActivityDTO> generateUserActivity(String subjectWhere, String encounterWhere, String enrolmentWhere, String userWhere) {
        SchemaMetadata schema = schemaMetadataRepository.getExistingSchemaMetadata();
        List<String> subjectTableNames = schema.getAllSubjectTableNames().stream().toList();
        List<String> encounterTableNames = schema.getAllEncounterTableNames().stream().toList();
        List<String> programEnrolmentTableNames = schema.getAllProgramEnrolmentTableNames().stream().toList();
        List<String> programEncounterTableNames = schema.getAllProgramEncounterTableNames().stream().toList();

        ST baseQuery = new ST("with registrations as (\n" +
                "   select last_modified_by_id, count(*) as registration_count\n" +
                "         from $schemaName.<first(subjectTableNames)>\n" +
                "         where is_voided = false\n" +
                "         $subjectWhere\n" +
                "         group by last_modified_by_id\n" +
                "<rest(subjectTableNames) : { subject | " +
                "union all \n"+
                "   select last_modified_by_id, count(*) as registration_count\n" +
                "         from $schemaName.<subject> \n" +
                "         where is_voided = false\n" +
                "         $subjectWhere \n" +
                "         group by last_modified_by_id \n}> " +
                " ),\n " +
                "encounters as (\n" +
                "   select last_modified_by_id, count(*) as encounter_count\n" +
                "         from $schemaName.<first(encounterTableNames)>\n" +
                "         where is_voided = false\n" +
                "         $encounterWhere\n" +
                "         group by last_modified_by_id\n" +
                "<rest(encounterTableNames) : { encounter | " +
                "union all \n"+
                "   select last_modified_by_id, count(*) as encounter_count\n" +
                "         from $schemaName.<encounter> \n" +
                "         where is_voided = false\n" +
                "         $encounterWhere \n" +
                "         group by last_modified_by_id \n}> " +
                " ),\n " +
                "enrolments as (\n" +
                "   select last_modified_by_id, count(*) as enrolment_count\n" +
                "         from $schemaName.<first(programEnrolmentTableNames)>\n" +
                "         where is_voided = false\n" +
                "         $enrolmentWhere\n" +
                "         group by last_modified_by_id\n" +
                "<rest(programEnrolmentTableNames) : { enrolment | " +
                "union all \n"+
                "   select last_modified_by_id, count(*) as enrolment_count\n" +
                "         from $schemaName.<enrolment> \n" +
                "         where is_voided = false\n" +
                "         $enrolmentWhere \n" +
                "         group by last_modified_by_id \n}> " +
                " ),\n " +
                "program_encounters as (\n" +
                "   select last_modified_by_id, count(*) as program_encounter_count\n" +
                "         from $schemaName.<first(programEncounterTableNames)>\n" +
                "         where is_voided = false\n" +
                "         $encounterWhere\n" +
                "         group by last_modified_by_id\n" +
                "<rest(programEncounterTableNames) : { programEncounter | " +
                "union all \n"+
                "   select last_modified_by_id, count(*) as program_encounter_count\n" +
                "         from $schemaName.<programEncounter> \n" +
                "         where is_voided = false\n" +
                "         $encounterWhere \n" +
                "         group by last_modified_by_id \n}> " +
                " ),\n " +
                "activity_table as (\n" +
                "   select u.id                                              as id,\n" +
                "       coalesce(u.name, u.username)                      as name,\n" +
                "       coalesce(registration_count, 0)                   as registration_count,\n" +
                "       coalesce(encounter_count, 0)                      as encounter_count,\n" +
                "       coalesce(enrolment_count, 0)                      as enrolment_count,\n" +
                "       coalesce(program_encounter_count, 0)              as program_encounter_count\n" +
                "   from $schemaName.users u\n" +
                "         left join registrations r on r.last_modified_by_id = u.id\n" +
                "         left join encounters e on e.last_modified_by_id = u.id\n" +
                "         left join enrolments enl on enl.last_modified_by_id = u.id\n" +
                "         left join program_encounters enc on enc.last_modified_by_id = u.id\n" +
                "   where (u.is_voided = false or u.is_voided isnull) and u.organisation_id notnull\n" +
                "       and coalesce(coalesce(registration_count, 0) + coalesce(encounter_count, 0) + coalesce(enrolment_count, 0) +\n" +
                "                coalesce(program_encounter_count, 0), 0) > 0\n" +
                "      $userWhere \n" +
                "),\n" +
                "final_table as (\n" +
                "   select id, name,\n"+
                "       sum(distinct registration_count) as registration_count, \n" +
                "       sum(distinct encounter_count) as encounter_count,\n" +
                "       sum(distinct enrolment_count) as enrolment_count,\n" +
                "       sum(distinct program_encounter_count) as program_encounter_count\n" +
                "   from activity_table \n" +
                "   group by id,name\n" +
                ")\n" +
                "select *, coalesce(coalesce(registration_count, 0) + coalesce(encounter_count, 0) + coalesce(enrolment_count, 0) +\n" +
                "       coalesce(program_encounter_count, 0), 0) as total\n" +
                "from final_table\n" +
                "order by 7 desc\n" +
                "limit 10;"
        );
        baseQuery.add("subjectTableNames", subjectTableNames)
                 .add("encounterTableNames", encounterTableNames)
                 .add("programEnrolmentTableNames", programEnrolmentTableNames)
                 .add("programEncounterTableNames", programEncounterTableNames);
        String query = baseQuery.render().replace("$schemaName", OrgIdentityContextHolder.getDbSchema())
               .replace("$subjectWhere", subjectWhere)
               .replace("$encounterWhere", encounterWhere)
               .replace("$enrolmentWhere", enrolmentWhere)
               .replace("$userWhere", userWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new UserActivityMapper());
    }

    public List<UserActivityDTO> generateUserSyncFailures(String syncTelemetryWhere, String userWhere) {
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
                .replace("${schemaName}", OrgIdentityContextHolder.getDbSchema())
                .replace("${syncTelemetryWhere}", syncTelemetryWhere)
                .replace("${userWhere}", userWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new UserCountMapper());
    }

    public List<AggregateReportResult> generateUserAppVersions(String userWhere) {
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
                .replace("${schemaName}", OrgIdentityContextHolder.getDbSchema())
                .replace("${userWhere}", userWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new AggregateReportMapper());
    }

    public List<AggregateReportResult> generateUserDeviceModels(String userWhere) {
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
                .replace("${schemaName}", OrgIdentityContextHolder.getDbSchema())
                .replace("${userWhere}", userWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new AggregateReportMapper());
    }

    public List<UserActivityDTO> generateUserDetails(String userWhere) {
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
                .replace("${schemaName}", OrgIdentityContextHolder.getDbSchema())
                .replace("${userWhere}", userWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new UserDetailsMapper());
    }

    public List<UserActivityDTO> generateLatestSyncs(String syncTelemetryWhere, String userWhere) {
        String baseQuery = "SELECT coalesce(u.name,u.username) as name, \n" +
                "           android_version, app_version, device_name, sync_start_time, sync_end_time, sync_status, sync_source\n" +
                "FROM public.sync_telemetry st\n" +
                "join ${schemaName}.users u on st.last_modified_by_id = u.id\n" +
                "where (u.is_voided = false or u.is_voided isnull) and u.organisation_id notnull\n" +
                "${syncTelemetryWhere}\n"+
                "${userWhere}\n"+
                "order by 6 desc\n" +
                "limit 10;\n";
        String query = baseQuery
                .replace("${schemaName}", OrgIdentityContextHolder.getDbSchema())
                .replace("${syncTelemetryWhere}", syncTelemetryWhere)
                .replace("${userWhere}", userWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new LatestSyncMapper());
    }

    public List<UserActivityDTO> generateMedianSync(String syncTelemetryWhere) {
        String baseQuery = "with weeks as (\n" +
                "    select day::date start_date, day::date+6 end_date\n" +
                "    ${syncTelemetryWhere}\n" +
                ")\n" +
                "select w.start_date, w.end_date, \n" +
                "    coalesce(percentile_cont(0.5) within group (order by (st.sync_end_time-st.sync_start_time)), '00:00:00') as median_sync_time\n" +
                "from weeks w\t\n" +
                "left join ${schemaName}.sync_telemetry st\n" +
                "on st.sync_start_time::date >= w.start_date and st.sync_end_time::date <= w.end_date\n" +
                "and st.sync_source = 'manual'\n" +
                "group by 1,2;";
        String query = baseQuery
                .replace("${schemaName}", OrgIdentityContextHolder.getDbSchema())
                .replace("${syncTelemetryWhere}", syncTelemetryWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new MedianSyncMapper());
    }

    public List<AggregateReportResult> generateCompletedVisitsOnTimeByProportion(String proportionCondition, String encounterWhere, String userWhere) {
        SchemaMetadata schema = schemaMetadataRepository.getExistingSchemaMetadata();
        List<String> encounterTableNames = schema.getAllEncounterTableNames().stream().toList();
        List<String> programEncounterTableNames = schema.getAllProgramEncounterTableNames().stream().toList();

        ST baseQuery = new ST("with program_enc_data as (\n " +
                "   select last_modified_by_id, \n" +
                "           count(*) filter ( where encounter_date_time \\<= max_visit_date_time )    visits_done_on_time,\n" +
                "           count(*) filter ( where encounter_date_time notnull and earliest_visit_date_time notnull ) total_scheduled\n" +
                "    from $schemaName.<first(programEncounterTableNames)>\n" +
                "    where is_voided = false\n" +
                "    $encounterWhere\n" +
                "    group by last_modified_by_id\n" +
                "<rest(programEncounterTableNames) : { programEncounter | " +
                "union all \n"+
                "   select last_modified_by_id,\n" +
                "           count(*) filter ( where encounter_date_time \\<= max_visit_date_time )    visits_done_on_time,\n" +
                "           count(*) filter ( where encounter_date_time notnull and earliest_visit_date_time notnull ) total_scheduled\n" +
                "    from $schemaName.<programEncounter> \n" +
                "    where is_voided = false\n" +
                "    $encounterWhere\n" +
                "    group by last_modified_by_id\n }> " +
                "),\n" +
                "general_enc_data as (\n" +
                "   select last_modified_by_id,\n" +
                "                count(*) filter ( where encounter_date_time \\<= max_visit_date_time )  visits_done_on_time,\n" +
                "                count(*) filter ( where encounter_date_time notnull and earliest_visit_date_time notnull ) total_scheduled\n" +
                "         from $schemaName.<first(encounterTableNames)>\n" +
                "         where is_voided = false\n" +
                "         $encounterWhere\n" +
                "         group by last_modified_by_id\n" +
                "<rest(encounterTableNames) : { encounter | " +
                "union all \n"+
                "   select last_modified_by_id,\n" +
                "           count(*) filter ( where encounter_date_time \\<= max_visit_date_time )    visits_done_on_time,\n" +
                "           count(*) filter ( where encounter_date_time notnull and earliest_visit_date_time notnull ) total_scheduled\n" +
                "    from $schemaName.<encounter>\n" +
                "    where is_voided = false\n" +
                "    $encounterWhere\n" +
                "    group by last_modified_by_id \n }>" +
                "),\n" +
                "total_visits_table as (\n" +
                "   select coalesce(u.name, u.username)    as indicator,\n" +
                "          coalesce(sum(distinct ged.visits_done_on_time), 0) as ged_visits_on_time,\n" +
                "          coalesce(sum(distinct ped.visits_done_on_time), 0) as ped_visits_on_time,\n" +
                "          coalesce(sum(distinct ged.total_scheduled), 0) as ged_total_scheduled,\n" +
                "          coalesce(sum(distinct ped.total_scheduled), 0) as ped_total_scheduled\n" +
                "   from $schemaName.users u \n" +
                "       left join general_enc_data ged on ged.last_modified_by_id = u.id\n" +
                "       left join program_enc_data ped on ped.last_modified_by_id = u.id\n" +
                "   where u.organisation_id notnull\n" +
                "       and (is_voided = false or is_voided isnull)\n" +
                "   $userWhere\n" +
                "   group by u.name, u.username	\n" +
                ")\n" +
                "select  indicator,\n" +
                "        ged_visits_on_time + ped_visits_on_time as count\n" +
                "from total_visits_table\n" +
                "where ged_visits_on_time + ped_visits_on_time > 0\n" +
                "       and ((ged_visits_on_time + ped_visits_on_time) /\n" +
                "nullif(ged_total_scheduled + ped_total_scheduled, 0)) $proportion_condition;"
        );
        baseQuery.add("programEncounterTableNames", programEncounterTableNames)
                 .add("encounterTableNames", encounterTableNames);
        String query = baseQuery.render().replace("$proportion_condition", proportionCondition)
                .replace("$schemaName", OrgIdentityContextHolder.getDbSchema())
                .replace("$encounterWhere", encounterWhere)
                .replace("$userWhere", userWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new AggregateReportMapper());
    }

    public List<AggregateReportResult> generateUserCancellingMostVisits(String encounterWhere, String userWhere) {
        SchemaMetadata schema = schemaMetadataRepository.getExistingSchemaMetadata();
        List<String> encounterTableNames = schema.getAllEncounterTableNames().stream().toList();
        List<String> programEncounterTableNames = schema.getAllProgramEncounterTableNames().stream().toList();

        ST baseQuery = new ST("with program_enc_data as (\n " +
                "    select last_modified_by_id,\n" +
                "           count(*) filter ( where cancel_date_time notnull ) cancelled_visits\n" +
                "    from $schemaName.<first(programEncounterTableNames)>\n" +
                "    where is_voided = false\n" +
                "    $encounterWhere\n" +
                "    group by last_modified_by_id\n" +
                "<rest(programEncounterTableNames) : { programEncounter | " +
                "union all \n"+
                "    select last_modified_by_id,\n" +
                "           count(*) filter ( where cancel_date_time notnull ) cancelled_visits\n" +
                "    from $schemaName.<programEncounter>\n" +
                "    where is_voided = false\n" +
                "    $encounterWhere\n" +
                "    group by last_modified_by_id \n }>" +
                "),\n" +
                "general_enc_data as (\n" +
                "    select last_modified_by_id,\n" +
                "           count(*) filter ( where cancel_date_time notnull ) cancelled_visits\n" +
                "         from $schemaName.<first(encounterTableNames)>\n" +
                "         where is_voided = false\n" +
                "         $encounterWhere\n" +
                "         group by last_modified_by_id\n" +
                "<rest(encounterTableNames) : { encounter | " +
                "union all \n"+
                "    select last_modified_by_id,\n" +
                "           count(*) filter ( where cancel_date_time notnull ) cancelled_visits\n" +
                "    from $schemaName.<encounter>\n" +
                "         where is_voided = false\n" +
                "         $encounterWhere\n" +
                "         group by last_modified_by_id \n }>" +
                "),\n" +
                "cancelled_visits_table as (\n" +
                "select coalesce(u.name, u.username)       as indicator,\n" +
                "       coalesce(sum(distinct ged.cancelled_visits), 0)  as ged_cancelled_visits,\n" +
                "       coalesce(sum(distinct ped.cancelled_visits), 0)  as ped_cancelled_visits\n" +
                "from $schemaName.users u\n" +
                "          left join general_enc_data ged on ged.last_modified_by_id = u.id\n" +
                "          left join program_enc_data ped on ped.last_modified_by_id = u.id\n" +
                "where u.organisation_id notnull\n" +
                "  and (is_voided = false or is_voided isnull)\n" +
                "  and coalesce(ged.cancelled_visits, 0) + coalesce(ped.cancelled_visits, 0) > 0 \n" +
                "  $userWhere\n" +
                "  group by u.name,u.username	\n" +
                ")\n" +
                "select  indicator,\n" +
                "        ged_cancelled_visits + ped_cancelled_visits as count\n" +
                "from cancelled_visits_table\n" +
                "order by ged_cancelled_visits + ped_cancelled_visits desc\n" +
                "limit 5;"
        );
        baseQuery.add("programEncounterTableNames", programEncounterTableNames)
                .add("encounterTableNames", encounterTableNames);
        String query = baseQuery.render().replace("$schemaName", OrgIdentityContextHolder.getDbSchema())
                .replace("$encounterWhere", encounterWhere)
                .replace("$userWhere", userWhere);

        runInOrgContext(() -> {
            jdbcTemplate.execute(query);
            return NullObject.instance();
        }, jdbcTemplate);

        return namedJdbcTemplate.query(query, new AggregateReportMapper());
    }
}
