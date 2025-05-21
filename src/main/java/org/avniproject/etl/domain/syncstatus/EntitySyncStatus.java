package org.avniproject.etl.domain.syncstatus;

import org.avniproject.etl.domain.Model;

import java.util.Date;

public class EntitySyncStatus extends Model {

    public enum Status {
        Running,
        Success,
        Failure
    }
    private Integer tableMetadataId;
    private Date lastSyncTime;
    private Status syncStatus;
    private String dbUser;
    private String schemaName;

    public EntitySyncStatus(Integer id, Integer tableMetadataId, Date lastSyncTime, Status syncStatus, String dbUser, String schemaName) {
        super(id);
        this.tableMetadataId = tableMetadataId;
        this.lastSyncTime = lastSyncTime;
        this.syncStatus = syncStatus;
        this.dbUser = dbUser;
        this.schemaName = schemaName;
    }

    public EntitySyncStatus(Integer tableMetadataId, Date lastSyncTime, Status syncStatus, String dbUser, String schemaName) {
        this(null, tableMetadataId, lastSyncTime, syncStatus, dbUser, schemaName);
    }

    public EntitySyncStatus startSync() {
        this.setSyncStatus(EntitySyncStatus.Status.Running);
        return this;
    }

    public void markSuccess(Date asOfDate) {
        this.syncStatus = Status.Success;
        this.lastSyncTime = asOfDate;
    }

    public void markFailure() {
        this.syncStatus = Status.Failure;
    }

    public Integer getTableMetadataId() {
        return tableMetadataId;
    }

    public Status getSyncStatus() {
        return syncStatus;
    }

    public void setSyncStatus(Status syncStatus) {
        this.syncStatus = syncStatus;
    }

    public void setTableMetadataId(Integer tableMetadataId) {
        this.tableMetadataId = tableMetadataId;
    }

    public Date getLastSyncTime() {
        return lastSyncTime;
    }

    public void setLastSyncTime(Date lastSyncTime) {
        this.lastSyncTime = lastSyncTime;
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getSchemaName() {
        return schemaName;
    }
}
