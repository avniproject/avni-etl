package org.avniproject.etl.contract;

import org.avniproject.etl.contract.backgroundJob.JobEntityType;
import org.avniproject.etl.contract.backgroundJob.JobGroup;

public class JobScheduleRequest {
    private String entityUUID;
    private JobEntityType jobEntityType;
    private JobGroup jobGroup = JobGroup.Sync;

    public String getEntityUUID() {
        return entityUUID;
    }

    public void setEntityUUID(String entityUUID) {
        this.entityUUID = entityUUID;
    }

    public JobEntityType getJobEntityType() {
        return jobEntityType;
    }

    public void setJobEntityType(JobEntityType jobEntityType) {
        this.jobEntityType = jobEntityType;
    }

    public JobGroup getJobGroup() {
        return jobGroup;
    }

    public void setJobGroup(JobGroup jobGroup) {
        this.jobGroup = jobGroup;
    }
}
