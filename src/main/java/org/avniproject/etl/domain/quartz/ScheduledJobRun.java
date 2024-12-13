package org.avniproject.etl.domain.quartz;

import jakarta.persistence.*;
import org.avniproject.etl.util.ExceptionUtil;
import org.quartz.JobDetail;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;

import java.util.Date;

@Entity
@Table(name = "scheduled_job_run")
public class ScheduledJobRun {
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", updatable = false, nullable = false)
    @Id
    private Long id;

    @Column
    private String jobName;

    @Column
    private Date startedAt;

    @Column
    private Date endedAt;

    @Column
    private String errorMessage;

    @Column
    private boolean success;

    public static ScheduledJobRun create(JobDetail jobDetail, Trigger trigger) {
        ScheduledJobRun scheduledJobRun = new ScheduledJobRun();
        scheduledJobRun.jobName = jobDetail.getKey().getName();
        scheduledJobRun.startedAt = new Date();
        return scheduledJobRun;
    }

    public void ended(JobExecutionException jobException, Trigger trigger) {
        this.endedAt = new Date();
        if (jobException != null)
            errorMessage = ExceptionUtil.getStackTraceAsString(getAppException(jobException));
    }

    private Throwable getAppException(Throwable throwable) {
        if (throwable.getCause() instanceof SchedulerException)
            return getAppException(throwable.getCause());
        return throwable.getCause();
    }

    public Long getId() {
        return id;
    }

    public String getJobName() {
        return jobName;
    }

    public Date getStartedAt() {
        return startedAt;
    }

    public Date getEndedAt() {
        return endedAt;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
