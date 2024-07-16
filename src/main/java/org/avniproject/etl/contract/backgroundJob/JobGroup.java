package org.avniproject.etl.contract.backgroundJob;

public enum JobGroup {
    Sync("SyncJobs", "SyncTriggers"), MediaAnalysis("MediaAnalysisJobs", "MediaAnalysisTriggers");

    String groupName, triggerName;

    JobGroup(String groupName, String triggerName) {
        this.groupName = groupName;
        this.triggerName = triggerName;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getTriggerName() {
        return triggerName;
    }
}
