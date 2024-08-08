package org.avniproject.etl.repository;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.dto.*;
import org.avniproject.etl.repository.service.MediaTableRepositoryService;
import org.avniproject.etl.repository.sql.MediaSearchQueryBuilder;
import org.avniproject.etl.repository.sql.Page;
import org.avniproject.etl.repository.sql.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInSchemaUserContext;

@Repository
public class MediaTableRepository {

    private final JdbcTemplate jdbcTemplate;
    private final MediaTableRepositoryService mediaTableRepositoryService;
    private final SchemaMetadataRepository schemaMetadataRepository;
    private final Logger logger = LoggerFactory.getLogger(MediaTableRepository.class);

    @Autowired
    MediaTableRepository(JdbcTemplate jdbcTemplate, MediaTableRepositoryService mediaTableRepositoryService, SchemaMetadataRepository schemaMetadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.mediaTableRepositoryService = mediaTableRepositoryService;
        this.schemaMetadataRepository = schemaMetadataRepository;
    }

    private List<ConceptFilterSearch> determineConceptFilterTablesAndColumns(List<ConceptFilter> conceptFilters) {
        logger.debug("searching concepts: " + conceptFilters);
        List<ColumnMetadata.ConceptType> textConceptSearchTypes = Arrays.asList(
            ColumnMetadata.ConceptType.Text,
            ColumnMetadata.ConceptType.Id,
            ColumnMetadata.ConceptType.Notes
        );

        SchemaMetadata schema = schemaMetadataRepository.getExistingSchemaMetadata();

        List<ConceptFilterSearch> conceptFilterTablesAndColumns = conceptFilters.stream().map(conceptFilter -> {
           TableMetadata table = schema.findTableByForm(conceptFilter.getFormUuid()).orElse(null);
           if (table != null) {
               ColumnMetadata column = table.findColumnMatchingConcept(conceptFilter.getConceptUuid()).orElse(null);
               return column != null ? new ConceptFilterSearch(table.getName(),
                       column.getName(),
                       conceptFilter.getValues(),
                       conceptFilter.getFrom(),
                       conceptFilter.getTo(),
                       column.getConceptType().equals(ColumnMetadata.ConceptType.Numeric),
                       !textConceptSearchTypes.contains(column.getConceptType()))
                   : null;
           }
           return null;
        }).toList();

        logger.debug("Returning conceptFilterTablesAndColumns: " + conceptFilterTablesAndColumns);
        return conceptFilterTablesAndColumns;
    }

    public List<MediaDTO> search(MediaSearchRequest mediaSearchRequest, Page page) {
        return searchInternal(mediaSearchRequest, page, (rs, rowNum) -> mediaTableRepositoryService.setMediaDto(rs));
    }

    private <T> List<T> searchInternal(MediaSearchRequest mediaSearchRequest, Page page, RowMapper<T> rowMapper) {
        List<ConceptFilterSearch> conceptFilterSearches = null;
        if (mediaSearchRequest.getConceptFilters() != null) {
            conceptFilterSearches = determineConceptFilterTablesAndColumns(mediaSearchRequest.getConceptFilters());
        }

        Query query = new MediaSearchQueryBuilder()
            .withPage(page)
            .withMediaSearchRequest(mediaSearchRequest)
            .withSearchConceptFilters(conceptFilterSearches)
            .build();
        return runInSchemaUserContext(() -> new NamedParameterJdbcTemplate(jdbcTemplate)
            .query(query.sql(), query.parameters(), rowMapper), jdbcTemplate);
    }

    public List<ImageData> getImageData(MediaSearchRequest mediaSearchRequest, Page page) {
        return searchInternal(mediaSearchRequest, page, (rs, rowNum) -> mediaTableRepositoryService.setImageData(rs));
    }

    public List<MediaCompactDTO> getAllMedia() {
        Query query = new MediaSearchQueryBuilder().allWithoutAnyLimitOrOffset().build();
        return runInSchemaUserContext(() -> new NamedParameterJdbcTemplate(jdbcTemplate)
                .query(query.sql(), query.parameters(), (rs, rowNum) -> mediaTableRepositoryService.setMediaCompactDTO(rs)), jdbcTemplate);
    }
}
