package org.avniproject.etl.dto;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

public final class MediaSearchRequest {
    private List<AddressRequest> addresses = new ArrayList<>();
    private List<String> subjectTypeNames = new ArrayList<>();
    private List<String> programNames = new ArrayList<>();
    private List<String> encounterTypeNames = new ArrayList<>();
    private List<ConceptDTO> imageConcepts = new ArrayList<>();
    private List<SyncValue> syncValues = new ArrayList<>();
    private List<ConceptFilter> conceptFilters = new ArrayList<>();
    private Date fromDate;
    private Date toDate;
    private String subjectName;
    private List<String> subjectNameTokens = new ArrayList<>();
    private boolean includeTotalCount = false;
    private BigInteger totalCount;

    public MediaSearchRequest() {
    }

    public List<AddressRequest> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<AddressRequest> addresses) {
        this.addresses = addresses;
    }

    public List<String> getAddressLevelTypes() {
        return this.getAddresses().stream().map(AddressRequest::getAddressLevelType).collect(Collectors.toList());
    }

    public List<String> getSubjectTypeNames() {
        return subjectTypeNames;
    }

    public void setSubjectTypeNames(List<String> subjectTypeNames) {
        this.subjectTypeNames = subjectTypeNames;
    }

    public List<String> getProgramNames() {
        return programNames;
    }

    public void setProgramNames(List<String> programNames) {
        this.programNames = programNames;
    }

    public List<String> getEncounterTypeNames() {
        return encounterTypeNames;
    }

    public void setEncounterTypeNames(List<String> encounterTypeNames) {
        this.encounterTypeNames = encounterTypeNames;
    }

    public List<ConceptDTO> getImageConcepts() {
        return imageConcepts;
    }

    public void setImageConcepts(List<ConceptDTO> imageConcepts) {
        this.imageConcepts = imageConcepts;
    }

    public List<SyncValue> getSyncValues() {
        return syncValues;
    }

    public void setSyncValues(List<SyncValue> syncValues) {
        this.syncValues = syncValues;
    }

    public List<ConceptFilter> getConceptFilters() {
        return conceptFilters;
    }

    public void setConceptFilters(List<ConceptFilter> conceptFilters) {
        this.conceptFilters = conceptFilters;
    }

    public Date getFromDate() {
        return fromDate;
    }

    public void setFromDate(Date fromDate) {
        this.fromDate = fromDate;
    }

    public Date getToDate() {
        return toDate;
    }

    public void setToDate(Date toDate) {
        this.toDate = toDate;
    }

    public String getSubjectName() {
        return subjectName;
    }

    public void setSubjectName(String subjectName) {
        this.subjectName = subjectName;
        this.setSubjectNameTokens(subjectName == null ? null : Arrays.stream(subjectName.split(" ")).toList());
    }

    public List<String> getSubjectNameTokens() {
        return subjectNameTokens;
    }

    private void setSubjectNameTokens(List<String> subjectNameTokens) {
        this.subjectNameTokens = subjectNameTokens;
    }

    public boolean isIncludeTotalCount() {
        return includeTotalCount;
    }

    public void setIncludeTotalCount(boolean includeTotalCount) {
        this.includeTotalCount = includeTotalCount;
    }

    public BigInteger getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(BigInteger totalCount) {
        this.totalCount = totalCount;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (MediaSearchRequest) obj;
        return Objects.equals(this.subjectTypeNames, that.subjectTypeNames) &&
                Objects.equals(this.programNames, that.programNames) &&
                Objects.equals(this.encounterTypeNames, that.encounterTypeNames) &&
                Objects.equals(this.imageConcepts, that.imageConcepts) &&
                Objects.equals(this.syncValues, that.syncValues) &&
                Objects.equals(this.fromDate, that.fromDate) &&
                Objects.equals(this.toDate, that.toDate) &&
                Objects.equals(this.conceptFilters, that.conceptFilters) &&
                Objects.equals(this.subjectName, that.subjectName) &&
                Objects.equals(this.totalCount, that.totalCount) &&
                Objects.equals(this.includeTotalCount, that.includeTotalCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subjectTypeNames, programNames, encounterTypeNames, imageConcepts, syncValues, fromDate, toDate, conceptFilters, subjectName, includeTotalCount, totalCount);
    }

    @Override
    public String toString() {
        return "MediaSearchRequest[" +
                "subjectTypeNames=" + subjectTypeNames + ", " +
                "programNames=" + programNames + ", " +
                "encounterTypeNames=" + encounterTypeNames + ", " +
                "imageConcepts=" + imageConcepts + ", " +
                "syncValues=" + syncValues + ", " +
                "conceptFilters=" + conceptFilters + ", " +
                "fromDate=" + fromDate + ", " +
                "toDate=" + toDate + ", " +
                "subjectName=" + subjectName + ", " +
                "includeTotalCount=" + includeTotalCount + ", " +
                "totalCount=" + totalCount + ']';
    }
}

