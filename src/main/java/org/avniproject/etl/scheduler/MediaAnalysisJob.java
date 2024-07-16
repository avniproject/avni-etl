package org.avniproject.etl.scheduler;

import org.avniproject.etl.config.ScheduledJobConfig;
import org.avniproject.etl.contract.backgroundJob.JobEntityType;
import org.avniproject.etl.service.MediaAnalysisService;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MediaAnalysisJob implements Job {
    private final MediaAnalysisService mediaAnalysisService;
    private final ScheduledJobConfig scheduledJobConfig;

    @Autowired
    public MediaAnalysisJob(MediaAnalysisService mediaAnalysisService, ScheduledJobConfig scheduledJobConfig) {
        this.mediaAnalysisService = mediaAnalysisService;
        this.scheduledJobConfig = scheduledJobConfig;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDetail jobDetail = context.getJobDetail();
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        String entityId = scheduledJobConfig.getEntityId(jobDetail);
        if (jobDataMap.get(ScheduledJobConfig.ENTITY_TYPE).equals(JobEntityType.Organisation))
            mediaAnalysisService.runFor(entityId);
        else mediaAnalysisService.runForOrganisationGroup(entityId);
    }
}
