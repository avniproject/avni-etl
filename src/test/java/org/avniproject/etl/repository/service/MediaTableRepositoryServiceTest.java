package org.avniproject.etl.repository.service;

import org.avniproject.etl.config.AmazonClientService;
import org.avniproject.etl.config.S3FileDoesNotExist;
import org.avniproject.etl.dto.MediaDTO;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MediaTableRepositoryServiceTest {

    @Test
    public void shouldReturnEmptySignedUrl() throws SQLException, S3FileDoesNotExist {
        AmazonClientService amazonClient = mock(AmazonClientService.class);
        when(amazonClient.generateMediaDownloadUrl(anyString())).thenThrow(new IllegalArgumentException());
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString(anyString())).thenReturn("dummy");
        when(resultSet.getLong(anyString())).thenReturn(1L);

        MediaDTO mediaDTO = new MediaTableRepositoryService(amazonClient).setMediaDto(resultSet);
        assertEquals(mediaDTO.signedUrl(), null);
        assertEquals(mediaDTO.signedThumbnailUrl(), null);
    }
}
