package org.avniproject.etl.repository;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
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

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        List<ConceptFilterSearch> allFilters = conceptFilters.stream()
                .flatMap(conceptFilter -> schema.findTablesByForm(conceptFilter.getFormUuid())
                        .flatMap(table -> table.getAllColumnsByConceptUuid(conceptFilter.getConceptUuid())
                                .map(column -> new ConceptFilterSearch(table.getName(),
                                        column.getName(),
                                        conceptFilter.getValues(),
                                        conceptFilter.getFrom(),
                                        conceptFilter.getTo(),
                                        column.getConceptType().equals(ColumnMetadata.ConceptType.Numeric),
                                        !textConceptSearchTypes.contains(column.getConceptType()))
                                )
                        )
                ).toList();

        Map<String, List<ConceptFilterSearch>> filtersByTable = allFilters.stream()
                .collect(Collectors.groupingBy(ConceptFilterSearch::getTableName));

        List<ConceptFilterSearch> optimizedFilters = new ArrayList<>();
        int aliasIndex = 0;
        
        for (Map.Entry<String, List<ConceptFilterSearch>> entry : filtersByTable.entrySet()) {
            String tableName = entry.getKey();
            List<ConceptFilterSearch> tableFilters = entry.getValue();
            
            Map<String, List<ConceptFilterSearch>> filtersByColumn = tableFilters.stream()
                    .collect(Collectors.groupingBy(ConceptFilterSearch::getColumnName));
            
            for (Map.Entry<String, List<ConceptFilterSearch>> columnEntry : filtersByColumn.entrySet()) {
                String columnName = columnEntry.getKey();
                List<ConceptFilterSearch> columnFilters = columnEntry.getValue();
                
                if (columnFilters.size() == 1) {
                    ConceptFilterSearch filter = columnFilters.get(0);
                    filter.setAliasIndex(aliasIndex++);
                    optimizedFilters.add(filter);
                } else {
                    ConceptFilterSearch firstFilter = columnFilters.get(0);
                    List<String> combinedValues = columnFilters.stream()
                            .flatMap(filter -> filter.getColumnValues() != null ? filter.getColumnValues().stream() : Stream.empty())
                            .distinct()
                            .collect(Collectors.toList());
                    
                    ConceptFilterSearch combinedFilter = new ConceptFilterSearch(
                            tableName,
                            columnName,
                            combinedValues,
                            firstFilter.getFrom(),
                            firstFilter.getTo(),
                            firstFilter.isNonStringValue(),
                            firstFilter.isExactSearch()
                    );
                    combinedFilter.setAliasIndex(aliasIndex++);
                    optimizedFilters.add(combinedFilter);
                }
            }
        }

        logger.debug("Returning optimized conceptFilterTablesAndColumns: " + optimizedFilters);
        return optimizedFilters;
    }

    public List<MediaDTO> search(MediaSearchRequest mediaSearchRequest, Page page) {
        return searchInternal(mediaSearchRequest, page, (rs, rowNum) -> mediaTableRepositoryService.setMediaDto(rs));
    }

    public BigInteger searchResultCount(MediaSearchRequest mediaSearchRequest) {
        if (mediaSearchRequest.getTotalCount() != null) return mediaSearchRequest.getTotalCount();
        List<ConceptFilterSearch> conceptFilterSearches = null;
        if (mediaSearchRequest.getConceptFilters() != null) {
            conceptFilterSearches = determineConceptFilterTablesAndColumns(mediaSearchRequest.getConceptFilters());
        }

        Query query = new MediaSearchQueryBuilder()
                .withMediaSearchRequest(mediaSearchRequest)
                .withSearchConceptFilters(conceptFilterSearches)
                .buildCountQuery();
        return runInSchemaUserContext(() -> new NamedParameterJdbcTemplate(jdbcTemplate)
                .queryForObject(query.sql(), query.parameters(), BigInteger.class), jdbcTemplate);
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
