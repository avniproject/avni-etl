package org.avniproject.etl.repository;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.dto.ConceptFilter;
import org.avniproject.etl.dto.ConceptFilterSearch;
import org.avniproject.etl.dto.MediaDTO;
import org.avniproject.etl.dto.MediaSearchRequest;
import org.avniproject.etl.repository.service.MediaTableRepositoryService;
import org.avniproject.etl.repository.sql.MediaSearchQueryBuilder;
import org.avniproject.etl.repository.sql.Page;
import org.avniproject.etl.repository.sql.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.stream.Stream;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Repository
public class MediaTableRepository {

    private final JdbcTemplate jdbcTemplate;
    private final MediaTableRepositoryService mediaTableRepositoryService;
    private final SchemaMetadataRepository schemaMetadataRepository;
    private final Logger log = LoggerFactory.getLogger(MediaTableRepository.class);

    @Autowired
    MediaTableRepository(JdbcTemplate jdbcTemplate, MediaTableRepositoryService mediaTableRepositoryService, SchemaMetadataRepository schemaMetadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.mediaTableRepositoryService = mediaTableRepositoryService;
        this.schemaMetadataRepository = schemaMetadataRepository;
    }

    private List<ConceptFilterSearch> determineConceptFilterTablesAndColumns(List<ConceptFilter> conceptFilters) {
        System.out.println("searching concepts: " + conceptFilters);
        List<ConceptFilterSearch> conceptFilterTablesAndColumns = new ArrayList<>();
        List<ColumnMetadata.ConceptType> supportedConceptSearchTypes = Arrays.asList(
            ColumnMetadata.ConceptType.Numeric,
            ColumnMetadata.ConceptType.Date,
            ColumnMetadata.ConceptType.SingleSelect,
            ColumnMetadata.ConceptType.MultiSelect
        );
        SchemaMetadata schema = schemaMetadataRepository.getExistingSchemaMetadata();
        List<TableMetadata> tablesToSearch = Stream.of(schema.getAllSubjectTables(),
                schema.getAllProgramEnrolmentTables(),
                schema.getAllProgramEncounterTables(),
                schema.getAllEncounterTables())
            .flatMap(Collection::stream)
            .toList();
        System.out.println("Searching tables: " + tablesToSearch);
        for (ConceptFilter conceptFilter: conceptFilters) {
            String conceptUuid = conceptFilter.getConceptUuid();
            for (TableMetadata tableMetadata: tablesToSearch) {
                Optional<ColumnMetadata> column = tableMetadata.getColumnMetadataList()
                    .stream()
                    .filter(columnMetadata -> Objects.equals(columnMetadata.getConceptUuid(), conceptUuid)
                        && supportedConceptSearchTypes.contains(columnMetadata.getConceptType())
                    ).findFirst();
                if (column.isPresent()) {
                    ColumnMetadata columnMetadata = column.get();
                    conceptFilterTablesAndColumns.add(new ConceptFilterSearch(tableMetadata.getName(),
                        columnMetadata.getName(), conceptFilter.getValues(),
                        conceptFilter.getFrom(), conceptFilter.getTo(),
                        columnMetadata.getConceptType().equals(ColumnMetadata.ConceptType.Numeric)));
                    break;
                }
            }
        }
        System.out.println("Returning conceptFilterTablesAndColumns: " + conceptFilterTablesAndColumns);
        return conceptFilterTablesAndColumns;
    }

    public List<MediaDTO> search(MediaSearchRequest mediaSearchRequest, Page page) {
        List<ConceptFilterSearch> conceptFilterSearches = null;
        if (mediaSearchRequest.getConceptFilters() != null) {
            conceptFilterSearches = determineConceptFilterTablesAndColumns(mediaSearchRequest.getConceptFilters());
        }

        Query query = new MediaSearchQueryBuilder()
                .withPage(page)
                .withMediaSearchRequest(mediaSearchRequest)
                .withSearchConceptFilters(conceptFilterSearches)
                .build();
        return runInOrgContext(() -> new NamedParameterJdbcTemplate(jdbcTemplate)
                .query(query.sql(), query.parameters(),
                        (rs, rowNum) -> mediaTableRepositoryService.setMediaDto(rs)), jdbcTemplate);
    }
}
