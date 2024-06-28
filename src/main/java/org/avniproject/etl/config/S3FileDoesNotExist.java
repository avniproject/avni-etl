package org.avniproject.etl.config;

public class S3FileDoesNotExist extends Exception {
    public S3FileDoesNotExist(String message) {
        super(message);
    }
}
