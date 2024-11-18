package org.avniproject.etl.service.backgroundJob;

import org.avniproject.etl.config.ScheduledJobConfig;
import org.avniproject.etl.contract.backgroundJob.EtlJobHistoryItem;
import org.avniproject.etl.contract.backgroundJob.EtlJobStatus;
import org.avniproject.etl.contract.backgroundJob.EtlJobSummary;
import org.avniproject.etl.contract.backgroundJob.JobGroup;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

@Service
public class ScheduledJobService {
    private final JdbcTemplate jdbcTemplate;
    private final Scheduler scheduler;
    private final ScheduledJobConfig scheduledJobConfig;

    private static final String HISTORY_QUERY = "select sjr.started_at, sjr.ended_at, sjr.error_message, sjr.success from qrtz_job_details qjd\n" + "    left outer join scheduled_job_run sjr on sjr.job_name = qjd.job_name\n" + "     where qjd.job_name = ? and qjd.job_group = ? order by 1 desc ";

    private static final String JOB_LIST_QUERY = "select organisationUUID, job_name from (SELECT unnest(string_to_array(?, ',')) as organisationUUID) foo\n" + "    left outer join qrtz_job_details qjd on organisationUUID = qjd.job_name where qjd.job_group = ? or qjd is null";

    @Autowired
    public ScheduledJobService(JdbcTemplate jdbcTemplate, Scheduler scheduler, ScheduledJobConfig scheduledJobConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.scheduler = scheduler;
        this.scheduledJobConfig = scheduledJobConfig;
    }

    public List<EtlJobStatus> getJobs(List<String> organisationUUIDs, JobGroup jobGroup) {
        String organisations = String.join(",", organisationUUIDs);
        return jdbcTemplate.query(JOB_LIST_QUERY, ps -> {
            ps.setString(1, organisations);
            ps.setString(2, jobGroup.getGroupName());
        }, new EtlJobStatusMapper());
    }

    public EtlJobSummary getLatestJobRun(String organisationUUID, JobGroup jobGroup) throws SchedulerException {
        String query = HISTORY_QUERY + "limit 1";
        List<EtlJobSummary> summaries = jdbcTemplate.query(query, ps -> {
            ps.setString(1, organisationUUID);
            ps.setString(2, jobGroup.getGroupName());
        }, new EtlJobLatestStatusResponseMapper());
        if (summaries.size() == 0) return null;
        EtlJobSummary etlJobSummary = summaries.get(0);

        JobDetail jobDetail = scheduler.getJobDetail(scheduledJobConfig.getJobKey(organisationUUID, jobGroup));
        etlJobSummary.setCreatedAt((Date) jobDetail.getJobDataMap().get(ScheduledJobConfig.JOB_CREATED_AT));
        return etlJobSummary;
    }

    public List<EtlJobHistoryItem> getJobHistory(String organisationUUID, JobGroup jobGroup) {
        return jdbcTemplate.query(HISTORY_QUERY, ps -> {
                ps.setString(1, organisationUUID);
                ps.setString(2, jobGroup.getGroupName());
        }, new EtlJobHistoryItemMapper());
    }

    static class EtlJobLatestStatusResponseMapper implements RowMapper<EtlJobSummary> {
        @Override
        public EtlJobSummary mapRow(ResultSet rs, int rowNum) throws SQLException {
            EtlJobSummary etlJobLatestStatusResponse = new EtlJobSummary();
            etlJobLatestStatusResponse.setLastStartAt(rs.getTimestamp(1));
            etlJobLatestStatusResponse.setLastEndedAt(rs.getTimestamp(2));
            etlJobLatestStatusResponse.setErrorMessage(rs.getString(3));
            return etlJobLatestStatusResponse;
        }
    }

    static class EtlJobHistoryItemMapper implements RowMapper<EtlJobHistoryItem> {
        @Override
        public EtlJobHistoryItem mapRow(ResultSet rs, int rowNum) throws SQLException {
            EtlJobHistoryItem etlJobLatestStatusResponse = new EtlJobHistoryItem();
            etlJobLatestStatusResponse.setStartedAt(rs.getTimestamp(1));
            etlJobLatestStatusResponse.setEndedAt(rs.getTimestamp(2));
            etlJobLatestStatusResponse.setErrorMessage(rs.getString(3));
            etlJobLatestStatusResponse.setSuccess(rs.getBoolean(4));
            return etlJobLatestStatusResponse;
        }
    }

    static class EtlJobStatusMapper implements RowMapper<EtlJobStatus> {
        @Override
        public EtlJobStatus mapRow(ResultSet rs, int rowNum) throws SQLException {
            EtlJobStatus etlJobStatus = new EtlJobStatus();
            etlJobStatus.setEntityUUID(rs.getString(1));
            etlJobStatus.setAnalyticsEnabled(rs.getObject(2) != null);
            return etlJobStatus;
        }
    }
}
