package org.avniproject.etl.repository.sql;

import org.avniproject.etl.domain.ContextHolder;
import org.avniproject.etl.dto.AddressRequest;
import org.avniproject.etl.dto.ConceptFilterSearch;
import org.avniproject.etl.dto.MediaSearchRequest;
import org.avniproject.etl.dto.SyncValue;
import org.stringtemplate.v4.ST;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.avniproject.etl.repository.sql.SqlFile.readFile;

public class MediaSearchQueryBuilder {
    private final ST template;
    private final Map<String, Object> parameters = new HashMap<>();
    private final String sqlTemplate = readFile("/sql/api/searchMedia.sql.st");

    public MediaSearchQueryBuilder() {
        this.template = new ST(sqlTemplate);
        addDefaultParameters();
    }

    public MediaSearchQueryBuilder withMediaSearchRequest(MediaSearchRequest request) {
        template.add("request", request);
        addParameters(request);
        return this;
    }

    public MediaSearchQueryBuilder withSearchConceptFilters(List<ConceptFilterSearch> conceptFilters) {
        System.out.println("Building with searchConceptFilters:" + conceptFilters);
        if (conceptFilters != null && !conceptFilters.isEmpty()) {
            template.add("joinTablesAndColumns", conceptFilters);
        }
        return this;
    }

    private void addParameters(MediaSearchRequest request) {
        addParameter("subjectTypeNames", request.getSubjectTypeNames());
        addParameter("programNames", request.getProgramNames());
        addParameter("encounterTypeNames", request.getEncounterTypeNames());
        addParameter("imageConcepts", request.getImageConcepts());
        addParameter("fromDate", request.getFromDate());
        addParameter("toDate", request.getToDate());

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

    public MediaSearchQueryBuilder withPage(Page page) {
        parameters.put("offset", page.offset());
        parameters.put("limit", page.limit());
        return this;
    }

    public Query build() {
        System.out.println(template.render());
        return new Query(template.render(), parameters);
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
        template.add("schemaName", ContextHolder.getDbSchema());
        parameters.put("offset", 0);
        parameters.put("limit", 10);
    }
}
