package org.avniproject.etl.dto;

import java.util.ArrayList;
import java.util.List;

public class ConceptFilter {
    private String conceptUuid;
    private List<String> values;
    private String from;
    private String to;

    public String getConceptUuid() {
        return conceptUuid;
    }

    public void setConceptUuid(String conceptUuid) {
        this.conceptUuid = conceptUuid;
    }

    public List<String> getValues() {
        if (values == null) {
            values = new ArrayList<>();
        }
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    @Override
    public String toString() {
        return "ConceptFilter[" +
            "conceptUuid=" + conceptUuid + ", " +
            "values=" + values + ", " +
            "from=" + from + ", " +
            "to=" + to + ']';
    }
}
