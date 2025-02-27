package org.avniproject.etl.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class OrganisationIdentity {
    private final String dbUser;
    private final String schemaName;
    private final String schemaUser;
    private final String mediaDirectory;
    private List<String> orgGroupOrgDbUsers = new ArrayList<>();
    private Date startTime;

    private OrganisationIdentity(String dbUser, String schemaName, String schemaUser, String mediaDirectory) {
        this.dbUser = dbUser;
        this.schemaName = schemaName;
        this.schemaUser = schemaUser;
        this.mediaDirectory = mediaDirectory;
    }

    /**
     * @param dbUser The database user who has access the source data of the organisation and is also a schema user
     * @param schemaName The destination schema name to be created/updated
     */
    public static OrganisationIdentity createForOrganisation(String dbUser, String schemaName, String mediaDirectory) {
        return new OrganisationIdentity(dbUser, schemaName, dbUser, mediaDirectory);
    }


    /**
     * @param dbUser The database user who has access the source data of the organisation
     * @param schemaName The destination schema name to be created/updated
     * @param schemaUser The destination user who will have access to the schema
     */
    public static OrganisationIdentity createForOrganisationGroup(String dbUser, String schemaName, String schemaUser) {
        return new OrganisationIdentity(dbUser, schemaName, schemaUser, null);
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public String toString() {
        return String.format("Schema: %s, DB User: %s, Schema User: %s, MediaDirectory: %s", schemaName, dbUser, schemaUser, mediaDirectory);
    }

    public String getDbUser() {
        return dbUser;
    }

    public String getSchemaUser() {
        return schemaUser;
    }

    public List<String> getUsersWithSchemaAccess() {
        if (dbUser.equals(schemaUser)) return Collections.singletonList(dbUser);
        List<String> dbUsers = new ArrayList<>();
        dbUsers.add(this.schemaUser);
        dbUsers.addAll(this.orgGroupOrgDbUsers);
        return dbUsers;
    }

    public void setOrgGroupOrgDbUsers(List<String> orgGroupOrgDbUsers) {
        this.orgGroupOrgDbUsers = orgGroupOrgDbUsers;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public String getMediaDirectory() {
        return mediaDirectory;
    }

    public boolean isPartOfGroup() {
        return this.orgGroupOrgDbUsers.size() > 1;
    }
}
