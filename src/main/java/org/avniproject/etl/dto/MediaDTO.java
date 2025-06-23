package org.avniproject.etl.dto;


import java.net.URL;

public record MediaDTO(String uuid, String subjectFirstName, String subjectLastName, String subjectMiddleName, String url, String conceptName, URL signedUrl, String thumbnailUrl, URL signedThumbnailUrl, String subjectTypeName, String programEnrolment, String encounterTypeName, String lastModifiedDateTime, String createdDateTime, String syncConcept1Name, String syncConcept2Name, String syncParameterValue1, String syncParameterValue2, Object address, long entityId, String mediaMetadata, String questionGroupConceptName, Long repeatableQuestionGroupIndex){
}
