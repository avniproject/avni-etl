package org.avniproject.etl.controller.backgroundJob;

import org.apache.log4j.Logger;
import org.avniproject.etl.config.ScheduledJobConfig;
import org.avniproject.etl.contract.JobScheduleRequest;
import org.avniproject.etl.contract.backgroundJob.*;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.repository.OrganisationRepository;
import org.avniproject.etl.scheduler.EtlJob;
import org.avniproject.etl.scheduler.MediaAnalysisJob;
import org.avniproject.etl.service.backgroundJob.ScheduledJobService;
import org.avniproject.etl.util.DateTimeUtil;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

@RestController
public class EtlJobController {
    private final Scheduler scheduler;
    private final ScheduledJobConfig scheduledJobConfig;
    private final OrganisationRepository organisationRepository;
    private final ScheduledJobService scheduledJobService;
    private static final Logger logger = Logger.getLogger(EtlJobController.class);

    @Autowired
    public EtlJobController(Scheduler scheduler, ScheduledJobConfig scheduledJobConfig, OrganisationRepository organisationRepository, ScheduledJobService scheduledJobService) {
        this.scheduler = scheduler;
        this.scheduledJobConfig = scheduledJobConfig;
        this.organisationRepository = organisationRepository;
        this.scheduledJobService = scheduledJobService;
    }

    @PreAuthorize("hasAnyAuthority('admin')")
    @GetMapping("/job/{entityUUID}")
    public ResponseEntity getJob(@PathVariable String entityUUID, @RequestParam(value="jobGroup", required = false) JobGroup jobGroup) throws SchedulerException {
        EtlJobSummary latestJobRun = scheduledJobService.getLatestJobRun(entityUUID, jobGroup != null ? jobGroup : JobGroup.Sync);
        if (latestJobRun == null) return ResponseEntity.notFound().build();
        return new ResponseEntity(latestJobRun, HttpStatus.OK);
    }

    @PreAuthorize("hasAnyAuthority('admin')")
    @PostMapping("/job/status")
    public List<EtlJobStatus> getStatuses(@RequestBody List<String> organisationUUIDs, @RequestParam(value="jobGroup", required = false) JobGroup jobGroup) {
        return scheduledJobService.getJobs(organisationUUIDs, jobGroup != null ? jobGroup : JobGroup.Sync);
    }

    @PreAuthorize("hasAnyAuthority('admin')")
    @GetMapping("/job/history/{entityUUID}")
    public List<EtlJobHistoryItem> getJobHistory(@PathVariable String entityUUID, @RequestParam(value="jobGroup", required = false) JobGroup jobGroup){
        return scheduledJobService.getJobHistory(entityUUID, jobGroup != null ? jobGroup : JobGroup.Sync);
    }

    @PreAuthorize("hasAnyAuthority('admin')")
    @PostMapping("/job")
    public ResponseEntity createJob(@RequestBody JobScheduleRequest jobScheduleRequest) throws SchedulerException {
        OrganisationIdentity organisationIdentity = null;
        List<OrganisationIdentity> organisationIdentitiesInGroup = new ArrayList<>();
        if (jobScheduleRequest.getJobEntityType().equals(JobEntityType.Organisation))
            organisationIdentity = organisationRepository.getOrganisation(jobScheduleRequest.getEntityUUID());
        else
            organisationIdentitiesInGroup = organisationRepository.getOrganisationGroup(jobScheduleRequest.getEntityUUID());

        ResponseEntity<String> jobScheduleValidationResult = validateRequest(jobScheduleRequest, organisationIdentity, organisationIdentitiesInGroup);
        if (jobScheduleValidationResult != null) return jobScheduleValidationResult;

        JobDetailImpl jobDetail = getJobDetail(jobScheduleRequest, organisationIdentity, organisationIdentitiesInGroup);
        scheduler.addJob(jobDetail, false);
        Trigger trigger = getTrigger(jobScheduleRequest, jobDetail);
        scheduler.scheduleJob(trigger);
        logger.info(String.format("%s type job Scheduled for %s:%s", jobScheduleRequest.getJobGroup(), jobScheduleRequest.getJobEntityType(), jobScheduleRequest.getEntityUUID()));
        return ResponseEntity.ok().body("Job Scheduled!");
    }

    private ResponseEntity<String> validateRequest(JobScheduleRequest jobScheduleRequest, OrganisationIdentity organisationIdentity, List<OrganisationIdentity> organisationIdentitiesInGroup) throws SchedulerException {
        if (organisationIdentity == null && organisationIdentitiesInGroup.size() == 0) {
            return ResponseEntity.badRequest().body(String.format("No such organisation or group exists: %s", jobScheduleRequest.getEntityUUID()));
        }
        EtlJobSummary latestJobRun = scheduledJobService.getLatestJobRun(jobScheduleRequest.getEntityUUID(), jobScheduleRequest.getJobGroup());
        if (latestJobRun != null) return ResponseEntity.badRequest().body("Job already present");
        if (!jobScheduleRequest.getJobGroup().equals(JobGroup.Sync)) {
            EtlJobSummary correspondingSyncJobRun = scheduledJobService.getLatestJobRun(jobScheduleRequest.getEntityUUID(), JobGroup.Sync);
            if (correspondingSyncJobRun == null)
                return ResponseEntity.badRequest().body("Sync Job has not been triggered for this Org / OrgGroup");
        }
        return null;
    }

    private Trigger getTrigger(JobScheduleRequest jobScheduleRequest, JobDetailImpl jobDetail) {
        SimpleScheduleBuilder scheduleBuilder = simpleSchedule().withIntervalInMinutes(jobScheduleRequest.getJobGroup().equals(JobGroup.Sync) ? scheduledJobConfig.getSyncRepeatIntervalInMinutes() : scheduledJobConfig.getMediaAnalysisRepeatIntervalInMinutes()).repeatForever();

        Trigger trigger = newTrigger().withIdentity(scheduledJobConfig.getTriggerKey(jobScheduleRequest.getEntityUUID(), jobScheduleRequest.getJobGroup())).forJob(jobDetail).withSchedule(scheduleBuilder).startAt(DateTimeUtil.nowPlusSeconds(5)).build();
        return trigger;
    }

    private JobDetailImpl getJobDetail(JobScheduleRequest jobScheduleRequest, OrganisationIdentity organisationIdentity, List<OrganisationIdentity> organisationIdentitiesInGroup) {
        JobDetailImpl jobDetail = new JobDetailImpl();
        jobDetail.setJobClass(jobScheduleRequest.getJobGroup().equals(JobGroup.Sync) ? EtlJob.class : MediaAnalysisJob.class);
        jobDetail.setDurability(true);
        jobDetail.setKey(scheduledJobConfig.getJobKey(jobScheduleRequest.getEntityUUID(), jobScheduleRequest.getJobGroup()));
        String truncatedDescription = getTruncatedDescription(organisationIdentity, organisationIdentitiesInGroup);
        jobDetail.setDescription(truncatedDescription);
        jobDetail.setGroup(jobScheduleRequest.getJobGroup().getGroupName());
        jobDetail.setName(jobScheduleRequest.getEntityUUID());
        JobDataMap jobDataMap = scheduledJobConfig.createJobData(jobScheduleRequest.getJobEntityType());
        jobDetail.setJobDataMap(jobDataMap);
        return jobDetail;
    }

    private String getTruncatedDescription(OrganisationIdentity organisationIdentity, List<OrganisationIdentity> organisationIdentitiesInGroup) {
        String orgGroupSchemaNames = organisationIdentitiesInGroup.stream().map(OrganisationIdentity::getSchemaName).collect(Collectors.joining(";"));
        String description = organisationIdentity == null ? "OrgGroup Schema names: " + orgGroupSchemaNames : organisationIdentity.toString();
        return StringUtils.truncate(description, 240);
    }

    @PreAuthorize("hasAnyAuthority('admin')")
    @DeleteMapping(value = "/job/{entityUUID}")
    public String deleteJob(@PathVariable String entityUUID, @RequestParam(value="jobGroup", required = false) JobGroup jobGroup) throws SchedulerException {
        boolean syncJobDeleted = scheduler.deleteJob(scheduledJobConfig.getJobKey(entityUUID, jobGroup != null ? jobGroup : JobGroup.Sync));
        String responseMsg = String.format("Sync Job Deleted: %s; ",syncJobDeleted);
        if (jobGroup != null && jobGroup == JobGroup.Sync) {
            EtlJobSummary mediaJobRun = scheduledJobService.getLatestJobRun(entityUUID, JobGroup.MediaAnalysis);
            if (mediaJobRun != null) {
                boolean mediaAnalysisJobDeleted = scheduler.deleteJob(scheduledJobConfig.getJobKey(entityUUID, JobGroup.MediaAnalysis));
                responseMsg.concat(String.format("MediaAnalysis Job Deleted: %s;", mediaAnalysisJobDeleted));
            }
        }
        return responseMsg;
    }
}
