package org.avniproject.etl.dto;

public class TableMetadataST {
    private String name;
    private String type;
    private String subjectTypeUuid;
    private String encounterTypeUuid;
    private String programUuid;
    private Boolean isPerson;

    public TableMetadataST(String name, String type, String subjectTypeUuid, String programUuid, String encounterTypeUuid, Boolean isPerson) {
        this.name = name;
        this.type = type;
        this.subjectTypeUuid = subjectTypeUuid;
        this.programUuid = programUuid;
        this.encounterTypeUuid = encounterTypeUuid;
        this.isPerson = isPerson;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSubjectTypeUuid() {
        return subjectTypeUuid;
    }

    public void setSubjectTypeUuid(String subjectTypeUuid) {
        this.subjectTypeUuid = subjectTypeUuid;
    }

    public Boolean getIsPerson() {
        return isPerson;
    }

    public void setIsPerson(Boolean isPerson) {
        this.isPerson = isPerson;
    }

    public String getProgramUuid() {
        return programUuid;
    }

    public void setProgramUuid(String programUuid) {
        this.programUuid = programUuid;
    }

    public String getEncounterTypeUuid() {
        return encounterTypeUuid;
    }

    public void setEncounterTypeUuid(String encounterTypeUuid) {
        this.encounterTypeUuid = encounterTypeUuid;
    }
}
