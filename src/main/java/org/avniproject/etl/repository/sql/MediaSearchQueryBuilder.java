package org.avniproject.etl.repository.sql;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.dto.*;
import org.springframework.util.CollectionUtils;
import org.stringtemplate.v4.ST;

import java.util.*;
import java.util.stream.Collectors;

import static org.avniproject.etl.repository.sql.SqlFile.readFile;

public class MediaSearchQueryBuilder {
    private final ST searchTemplate;
    private final ST countTemplate;
    private final Map<String, Object> parameters = new HashMap<>();
    private final static String countSqlTemplate = readFile("/sql/api/searchMedia.sql.st");
    private final static String searchSqlTemplate = readFile("/sql/api/countMedia.sql.st");
    private static final Logger logger = Logger.getLogger(MediaSearchQueryBuilder.class);

    public MediaSearchQueryBuilder() {
        this.searchTemplate = new ST(countSqlTemplate);
        this.countTemplate = new ST(searchSqlTemplate);
        addDefaultParameters();
    }

    public MediaSearchQueryBuilder withMediaSearchRequest(MediaSearchRequest request) {
        searchTemplate.add("request", request);
        countTemplate.add("request", request);
        addParameters(request);
        return this;
    }

    public MediaSearchQueryBuilder withSearchConceptFilters(List<ConceptFilterSearch> conceptFilters) {
        logger.debug("Building with searchConceptFilters:" + conceptFilters);
        if (conceptFilters != null && !conceptFilters.isEmpty()) {
            searchTemplate.add("joinTablesAndColumns", conceptFilters);
            countTemplate.add("joinTablesAndColumns", conceptFilters);
        }
        return this;
    }

    private void addParameters(MediaSearchRequest request) {
        addParameter("subjectTypeNames", request.getSubjectTypeNames());
        addParameter("programNames", request.getProgramNames());
        addParameter("encounterTypeNames", request.getEncounterTypeNames());
        addParameter("imageConcepts", getConceptNames(request.getImageConcepts()));
        addParameter("fromDate", request.getFromDate());
        addParameter("toDate", request.getToDate());
        addParameter("subjectName", request.getSubjectName());
        addParameter("subjectNameTokens", request.getSubjectNameTokens());

        List<AddressRequest> addressRequests = request.getAddresses();
        for (int index = 0; index < addressRequests.size(); index++) {
            addParameter("addressLevelIds_" + index, addressRequests.get(index).getAddressLevelIds());
        }

        List<SyncValue> syncValues = request.getSyncValues();
        for (int index = 0; index < syncValues.size(); index++) {
            SyncValue syncValue = syncValues.get(index);
            addParameter("syncConceptName_" + index, syncValue.getSyncConceptName());
            addParameter("syncConceptValues_" + index, syncValue.getSyncConceptValues());
        }
    }

    private List<String> getConceptNames(List<ConceptDTO> imageConcepts) {
        if(CollectionUtils.isEmpty(imageConcepts)) {
            return Collections.emptyList();
        }
        return imageConcepts.stream().map(concept -> concept.getName()).collect(Collectors.toList());
    }

    public MediaSearchQueryBuilder withPage(Page page) {
        parameters.put("offset", page.offset());
        parameters.put("limit", page.limit());
        return this;
    }

    public MediaSearchQueryBuilder allWithoutAnyLimitOrOffset() {
        searchTemplate.add("joinTablesAndColumns", null);
        searchTemplate.add("request", null);
        parameters.put("offset", 0);
        parameters.put("limit", Long.MAX_VALUE);
        return this;
    }

    public Query build() {
        String str = searchTemplate.render();
        logger.debug(str);
        return new Query(str, parameters);
    }

    public Query buildCountQuery() {
        String str = countTemplate.render();
        logger.debug(str);
        return new Query(str, parameters);
    }

    private void addParameter(String key, List value) {
        if (value != null && !value.isEmpty()) {
            parameters.put(key, value);
        }
    }

    private void addParameter(String key, Object value) {
        if (value != null) {
            parameters.put(key, value);
        }
    }

    private void addDefaultParameters() {
        searchTemplate.add("schemaName", OrgIdentityContextHolder.getDbSchema());
        countTemplate.add("schemaName", OrgIdentityContextHolder.getDbSchema());
        parameters.put("offset", 0);
        parameters.put("limit", 10);
    }
}
