package org.avniproject.etl.scheduler;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.quartz.ScheduledJobRun;
import org.avniproject.etl.repository.quartz.ScheduledJobRunRepository;
import org.avniproject.etl.service.EtlService;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EtlJobListener implements JobListener {
    private final ScheduledJobRunRepository scheduledJobRunRepository;
    private static final Logger log = Logger.getLogger(EtlService.class);

    @Autowired
    public EtlJobListener(ScheduledJobRunRepository scheduledJobRunRepository) {
        this.scheduledJobRunRepository = scheduledJobRunRepository;
    }

    @Override
    public String getName() {
        return "ScheduledJobRunPersister";
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {
        try {
            JobDetail jobDetail = context.getJobDetail();
            ScheduledJobRun scheduledJobRun = ScheduledJobRun.
                    create(jobDetail, context.getTrigger());
            scheduledJobRunRepository.save(scheduledJobRun);
        } catch (Exception exception) {
            log.error("Error while creating scheduled job run", exception);
            throw exception;
        }
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {
    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        try {
            if (jobException != null) {
                log.error("Error in job run", jobException);
            }
            ScheduledJobRun scheduledJobRun = scheduledJobRunRepository.getLastRun(context.getJobDetail().getKey().getName());
            scheduledJobRun.ended(jobException, context.getTrigger());
            scheduledJobRun.setSuccess(jobException == null);
            scheduledJobRunRepository.save(scheduledJobRun);
        } catch (Exception exception) {
            log.error("Error while capturing job result", exception);
            throw exception;
        }
    }
}
