package org.avniproject.etl.service;

import org.avniproject.etl.dto.*;
import org.avniproject.etl.repository.AddressRepository;
import org.avniproject.etl.repository.MediaTableRepository;
import org.avniproject.etl.repository.sql.Page;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;
import org.avniproject.etl.domain.metadata.TableMetadata;

import java.util.List;

@Service
public class MediaService {

    private final MediaTableRepository mediaTableRepository;
    private final AddressRepository addressRepository;
    private final RestTemplate restTemplate;

    @Value("${media.server.downloadRequestURL}")
    private String mediaServerDownloadRequestURL;

    @Autowired
    public MediaService(MediaTableRepository mediaTableRepository, AddressRepository addressRepository) {
        this.mediaTableRepository = mediaTableRepository;
        this.addressRepository = addressRepository;
        this.restTemplate = new RestTemplate();
    }

    @Transactional (readOnly = true)
    public MediaSearchResponseDTO search(MediaSearchRequest mediaSearchRequest, Page page) {
        if (addressRepository.doAllAddressLevelTypeNamesExist(mediaSearchRequest.getAddressLevelTypes())) {
            return new MediaSearchResponseDTO(page, mediaTableRepository.search(mediaSearchRequest, page), mediaSearchRequest.isIncludeTotalCount() ? mediaTableRepository.searchResultCount(mediaSearchRequest) : null);
        }

        throw new IllegalArgumentException("Address level type names are incorrect");
    }

    public void createDownloadRequest(DownloadAllMediaRequest downloadAllMediaRequest) {
        MediaSearchRequest mediaSearchRequest = downloadAllMediaRequest.getMediaSearchRequest();
        if (addressRepository.doAllAddressLevelTypeNamesExist(mediaSearchRequest.getAddressLevelTypes())) {
            Page page = new Page(0, 1000);
            List<ImageData> imageData = mediaTableRepository.getImageData(mediaSearchRequest, page);
            DownloadRequest downloadRequest = new DownloadRequest(downloadAllMediaRequest.getUsername(),
                                                                downloadAllMediaRequest.getDescription(),
                                                                downloadAllMediaRequest.getAddressLevelTypes(),
                                                                imageData);
            HttpEntity<DownloadRequest> request = new HttpEntity<>(downloadRequest);
            restTemplate.postForLocation(mediaServerDownloadRequestURL, request);
            return;
        }
        throw new IllegalArgumentException("Address level type names are incorrect");
    }

    /**
     * Determines the appropriate subject ID column to use for joining with the subject table.
     * Handles special cases like when the entity table is the subject table itself.
     *
     * @param tableMetadata The metadata for the table we're working with
     * @return The name of the column to use for the subject ID join
     */
    public String determineSubjectIdColumn(TableMetadata tableMetadata) {
        // If this is the individual/subject table itself, use 'id' for self-join
        if (tableMetadata.isSubjectTable() || "individual".equals(tableMetadata.getName())) {
            return "id";
        }
        
        // Check if table has subject_id column
        if (tableMetadata.hasColumn("subject_id")) {
            return "subject_id";
        }
        
        // Default to individual_id for backward compatibility
        return "individual_id";
    }
}
