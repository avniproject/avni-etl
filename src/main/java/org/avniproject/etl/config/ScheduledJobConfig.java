package org.avniproject.etl.config;

import org.avniproject.etl.contract.backgroundJob.JobEntityType;
import org.avniproject.etl.contract.backgroundJob.JobGroup;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.TriggerKey;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class ScheduledJobConfig {
    public static final String JOB_CREATED_AT = "CreatedAt";
    public static final String ENTITY_TYPE = "EntityType";

    @Value("${avni.scheduledJob.sync.repeatIntervalInMinutes}")
    private int repeatIntervalInMinutes;

    @Value("${avni.scheduledJob.mediaAnalysis.repeatIntervalInMinutes}")
    private int mediaAnalysisRepeatIntervalInMinutes;

    public TriggerKey getTriggerKey(String organisationUUID, JobGroup jobGroup) {
        return new TriggerKey(organisationUUID, jobGroup.getTriggerName());
    }

    public int getSyncRepeatIntervalInMinutes() {
        return repeatIntervalInMinutes;
    }

    public int getMediaAnalysisRepeatIntervalInMinutes() {
        return mediaAnalysisRepeatIntervalInMinutes;
    }

    public JobKey getJobKey(String organisationUUID, JobGroup jobGroup) {
        return new JobKey(organisationUUID, jobGroup.getGroupName());
    }

    public String getEntityId(JobDetail jobDetail) {
        return jobDetail.getKey().getName();
    }

    public JobDataMap createJobData(JobEntityType jobEntityType) {
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(ScheduledJobConfig.JOB_CREATED_AT, new Date());
        jobDataMap.put(ScheduledJobConfig.ENTITY_TYPE, jobEntityType);
        return jobDataMap;
    }
}
