package org.avniproject.etl.dto;

import java.util.Objects;

public class MediaAnalysisVO {

    String uuid;
    String image_url;
    boolean isValidUrl;
    boolean isPresentInStorage;
    boolean isThumbnailGenerated;
    boolean isHavingDuplicates;

    public MediaAnalysisVO(String uuid, String image_url, boolean isValidUrl, boolean isPresentInStorage, boolean isThumbnailGenerated, boolean isHavingDuplicates) {
        this.uuid = uuid;
        this.image_url = image_url;
        this.isValidUrl = isValidUrl;
        this.isPresentInStorage = isPresentInStorage;
        this.isThumbnailGenerated = isThumbnailGenerated;
        this.isHavingDuplicates = isHavingDuplicates;
    }

    public String getUuid() {
        return uuid;
    }

    public String getImage_url() {
        return image_url;
    }

    public boolean isValidUrl() {
        return isValidUrl;
    }

    public boolean isPresentInStorage() {
        return isPresentInStorage;
    }

    public boolean isThumbnailGenerated() {
        return isThumbnailGenerated;
    }

    public boolean isHavingDuplicates() {
        return isHavingDuplicates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MediaAnalysisVO)) return false;
        MediaAnalysisVO that = (MediaAnalysisVO) o;
        return getUuid().equals(that.getUuid()) && getImage_url().equals(that.getImage_url());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUuid(), getImage_url());
    }
}
