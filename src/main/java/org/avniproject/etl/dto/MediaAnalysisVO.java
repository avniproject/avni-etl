package org.avniproject.etl.dto;

import java.util.Objects;

public class MediaAnalysisVO {

    String uuid;
    String image_url;
    boolean isValidUrl;
    boolean isPresentInStorage;
    boolean isThumbnailGenerated;

    public MediaAnalysisVO(String uuid, String image_url, boolean isValidUrl, boolean isPresentInStorage, boolean isThumbnailGenerated) {
        this.uuid = uuid;
        this.image_url = image_url;
        this.isValidUrl = isValidUrl;
        this.isPresentInStorage = isPresentInStorage;
        this.isThumbnailGenerated = isThumbnailGenerated;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MediaAnalysisVO)) return false;
        MediaAnalysisVO that = (MediaAnalysisVO) o;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }
}
