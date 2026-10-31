package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * #173 - two live form elements on one form pointing at the same concept produced two columns of the
 * same name, and the flat-table insert failed with "column specified more than once".
 */
public class TableMetadataMapperDuplicateConceptTest {
    @BeforeEach
    public void setUp() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("udgamh", "udgamh", "udgamh"));
    }

    @AfterEach
    public void tearDown() {
        OrgIdentityContextHolder.clear();
    }

    private Map<String, Object> row(int conceptId, String conceptUuid, String conceptName) {
        Map<String, Object> row = new HashMap<>();
        row.put("form_mapping_uuid", "fm-1");
        row.put("form_uuid", "form-1");
        row.put("subject_type_name", "Garment Partners");
        row.put("subject_type_uuid", "st-1");
        row.put("subject_type_type", "Person");
        row.put("subject_type_allow_middle_name", false);
        row.put("table_type", "IndividualProfile");
        row.put("concept_id", conceptId);
        row.put("concept_uuid", conceptUuid);
        row.put("concept_name", conceptName);
        row.put("concept_voided", false);
        row.put("element_type", "Text");
        return row;
    }

    @Test
    public void shouldCreateOneColumnWhenTwoFormElementsMapToTheSameConcept() {
        List<Map<String, Object>> rows = List.of(
                row(1, "phone-uuid", "Phone Number"),
                row(1, "phone-uuid", "Phone Number"),
                row(2, "other-uuid", "Other"));

        TableMetadata table = new TableMetadataMapper().create(rows);

        List<String> names = table.getColumnMetadataList().stream().map(ColumnMetadata::getName).toList();
        assertEquals(1, names.stream().filter("Phone Number"::equals).count(), "columns: " + names);
        assertEquals(1, names.stream().filter("Other"::equals).count());
        assertEquals(names.size(), names.stream().distinct().count(), "columns: " + names);
    }
}
