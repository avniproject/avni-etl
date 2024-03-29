package org.avniproject.etl.dto;

import org.joda.time.DateTime;

public class UserActivityDTO {

    private String tableName;
    private String tableType;
    private String userName;
    private Long id;
    private Long registrationCount;
    private Long programEnrolmentCount;
    private Long programEncounterCount;
    private Long generalEncounterCount;
    private Long count;
    private String androidVersion;
    private String appVersion;
    private String deviceModel;
    private String syncStatus;
    private String syncSource;
    private DateTime syncStart;
    private DateTime syncEnd;
    private DateTime lastSuccessfulSync;
    private String medianSync;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getRegistrationCount() {
        return registrationCount;
    }

    public void setRegistrationCount(Long registrationCount) {
        this.registrationCount = registrationCount;
    }

    public Long getProgramEnrolmentCount() {
        return programEnrolmentCount;
    }

    public void setProgramEnrolmentCount(Long programEnrolmentCount) {
        this.programEnrolmentCount = programEnrolmentCount;
    }

    public Long getProgramEncounterCount() {
        return programEncounterCount;
    }

    public void setProgramEncounterCount(Long programEncounterCount) {
        this.programEncounterCount = programEncounterCount;
    }

    public Long getGeneralEncounterCount() {
        return generalEncounterCount;
    }

    public void setGeneralEncounterCount(Long generalEncounterCount) {
        this.generalEncounterCount = generalEncounterCount;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public String getAndroidVersion() {
        return androidVersion;
    }

    public void setAndroidVersion(String androidVersion) {
        this.androidVersion = androidVersion;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public String getDeviceModel() {
        return deviceModel;
    }

    public void setDeviceModel(String deviceModel) {
        this.deviceModel = deviceModel;
    }

    public DateTime getLastSuccessfulSync() {
        return lastSuccessfulSync;
    }

    public void setLastSuccessfulSync(DateTime lastSuccessfulSync) {
        this.lastSuccessfulSync = lastSuccessfulSync;
    }

    public DateTime getSyncStart() {
        return syncStart;
    }

    public void setSyncStart(DateTime syncStart) {
        this.syncStart = syncStart;
    }

    public DateTime getSyncEnd() {
        return syncEnd;
    }

    public void setSyncEnd(DateTime syncEnd) {
        this.syncEnd = syncEnd;
    }

    public String getSyncStatus() {
        return syncStatus;
    }

    public void setSyncStatus(String syncStatus) {
        this.syncStatus = syncStatus;
    }

    public String getSyncSource() {
        return syncSource;
    }

    public void setSyncSource(String syncSource) {
        this.syncSource = syncSource;
    }

    public String getMedianSync() {
        return medianSync;
    }

    public void setMedianSync(String medianSync) {
        this.medianSync = medianSync;
    }
}
