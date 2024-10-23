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
    private final ST template;
    private final Map<String, Object> parameters = new HashMap<>();
    private final static String sqlTemplate = readFile("/sql/api/searchMedia.sql.st");
    private final static String withoutAddressFilterSqlTemplate = readFile("/sql/api/searchMediaWithoutAddressFilter.sql.st");
    private static final Logger logger = Logger.getLogger(MediaSearchQueryBuilder.class);

    public MediaSearchQueryBuilder() {
        this.template = new ST(sqlTemplate);
        addDefaultParameters();
    }

    public MediaSearchQueryBuilder(MediaSearchRequest mediaSearchRequest) {
        this.template = mediaSearchRequest.getAddresses().isEmpty() ? new ST(withoutAddressFilterSqlTemplate) : new ST(sqlTemplate);
        addDefaultParameters();
    }

    public MediaSearchQueryBuilder withMediaSearchRequest(MediaSearchRequest request) {
        template.add("request", request);
        addParameters(request);
        return this;
    }

    public MediaSearchQueryBuilder withSearchConceptFilters(List<ConceptFilterSearch> conceptFilters) {
        logger.debug("Building with searchConceptFilters:" + conceptFilters);
        if (conceptFilters != null && !conceptFilters.isEmpty()) {
            template.add("joinTablesAndColumns", conceptFilters);
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
        template.add("joinTablesAndColumns", null);
        template.add("request", null);
        parameters.put("offset", 0);
        parameters.put("limit", Long.MAX_VALUE);
        return this;
    }

    public Query build() {
        String str = template.render();
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
        template.add("schemaName", OrgIdentityContextHolder.getDbSchema());
        parameters.put("offset", 0);
        parameters.put("limit", 10);
    }
}
