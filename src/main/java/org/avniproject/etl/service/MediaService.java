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
}
