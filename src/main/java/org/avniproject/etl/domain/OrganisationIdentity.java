package org.avniproject.etl.domain;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Important when OrganisationIdentity is used as an organisation group.
 * In case of organisations group n+1 schemas are maintained. n = one for each organisation in the group and 1 for the group itself. Hence it is important to know how the fields are used for organisation group.
 * dbUser = This is database user that will be used to QUERY the source data and hence that user's RLS will be applied. So this will the organisation's dbUser and not the group dbUser.
 * schemaName = The schema name that will be created/updated. This is not the same as the schema of the organisation in which the data resides. This is not the schema of the organisation but the schema of organisation group
 * schemaUser = The schema user is the db user for the organisation group. This is the user that will be used to INSERT/UPDATE/DELETE the data during ETL sync.
 */
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
