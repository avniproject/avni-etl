package org.avniproject.etl.repository;

/**
 * Enum containing keys used in the post-ETL sync status table.
 */
public enum PostETLSyncStatusKeys {
    CUTOFF_TIME("previous_cutoff_datetime");

    private final String key;

    PostETLSyncStatusKeys(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
