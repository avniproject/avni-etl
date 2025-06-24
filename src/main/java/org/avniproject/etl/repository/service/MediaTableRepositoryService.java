package org.avniproject.etl.repository.service;

import org.avniproject.etl.config.AmazonClientService;
import org.avniproject.etl.config.S3FileDoesNotExist;
import org.avniproject.etl.dto.ImageData;
import org.avniproject.etl.dto.MediaCompactDTO;
import org.avniproject.etl.dto.MediaDTO;
import org.avniproject.etl.util.Utils;
import org.springframework.stereotype.Service;

import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;

@Service
public class MediaTableRepositoryService {
    public static final int INTEGER_CONSTANT_ONE = 1;
    public static final int INT_INDEX_WHEN_NOT_FOUND = -1;
    public static final int BEGIN_INDEX = 0;
    private final AmazonClientService amazonClientService;

    public MediaTableRepositoryService(AmazonClientService amazonClientService) {
        this.amazonClientService = amazonClientService;
    }

    public MediaCompactDTO setMediaCompactDTO(ResultSet rs) {
        try {
            String imageUrl = rs.getString("image_url");
            String uuid = rs.getString("uuid");
            String imageUUID = getImageUUID(imageUrl);
            String compositeUUID = uuid + "#" + imageUUID;
            return new MediaCompactDTO(
                    compositeUUID,
                    uuid,
                    imageUrl
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public MediaDTO setMediaDto(ResultSet rs) {
        try {
            String imageUrl = rs.getString("image_url");
            String thumbnailUrl = Utils.getThumbnailUrl(imageUrl);

            URL signedImageUrl = null, signedThumbnailUrl = null;

            try {
                signedImageUrl = amazonClientService.generateMediaDownloadUrl(imageUrl);
                try {
                    signedThumbnailUrl = amazonClientService.generateMediaDownloadUrl(thumbnailUrl);
                } catch (IllegalArgumentException | S3FileDoesNotExist exception) {
                    //Ignore and move on. Thumbnail will be broken
                }
            } catch (IllegalArgumentException | S3FileDoesNotExist exception) {
                //Ignore and move on. Image will be null
            }

            String uuid = rs.getString("uuid");
            String imageUUID = getImageUUID(imageUrl);
            String compositeUUID = uuid + "#" + imageUUID;
            String repeatableQuestionGroupIndex = rs.getString("repeatable_question_group_index");
            return new MediaDTO(
                    compositeUUID,
                    rs.getString("subject_first_name"),
                    rs.getString("subject_last_name"),
                    rs.getString("subject_middle_name"),
                    imageUrl,
                    rs.getString("concept_name"),
                    signedImageUrl,
                    thumbnailUrl,
                    signedThumbnailUrl,
                    rs.getString("subject_type_name"),
                    rs.getString("program_name"),
                    rs.getString("encounter_type_name"),
                    rs.getString("last_modified_date_time"),
                    rs.getString("created_date_time"),
                    rs.getString("sync_parameter_key1"),
                    rs.getString("sync_parameter_key2"),
                    rs.getString("sync_parameter_value1"),
                    rs.getString("sync_parameter_value2"),
                    rs.getString("address"),
                    rs.getLong("entity_id"),
                    rs.getString("media_metadata"),
                    rs.getString("question_group_concept_name"),
                    repeatableQuestionGroupIndex == null ? null : Long.parseLong(repeatableQuestionGroupIndex));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ImageData setImageData(ResultSet rs) {
        try {
            String imageUrl = rs.getString("image_url");
            String imageUUID = getImageUUID(imageUrl);
            String compositeImageUUID = rs.getString("uuid") + "#" + imageUUID;
            return new ImageData(compositeImageUUID,
                    rs.getString("subject_first_name"),
                    rs.getString("subject_last_name"),
                    rs.getString("subject_middle_name"),
                    imageUrl,
                    rs.getString("concept_name"),
                    rs.getString("subject_type_name"),
                    rs.getString("program_name"),
                    rs.getString("encounter_type_name"),
                    rs.getString("sync_parameter_key1"),
                    rs.getString("sync_parameter_key2"),
                    rs.getString("sync_parameter_value1"),
                    rs.getString("sync_parameter_value2"),
                    rs.getString("address"));
        }catch(SQLException e) {
            throw new Error("Error:" + e.getMessage());
        }
    }

    private static String getImageUUID(String imageUrl) {
        String imageUUID = imageUrl;
        int startOfUUid = imageUrl.lastIndexOf('/');
        if(startOfUUid > INT_INDEX_WHEN_NOT_FOUND) {
            String imageUUIDWithExtension = imageUrl.substring(startOfUUid + INTEGER_CONSTANT_ONE);
            int endOfUUID = imageUUIDWithExtension.indexOf('.');
            imageUUID = endOfUUID > INT_INDEX_WHEN_NOT_FOUND ? imageUUIDWithExtension.substring(BEGIN_INDEX, endOfUUID) : imageUUIDWithExtension;
        }
        return imageUUID;
    }
}
