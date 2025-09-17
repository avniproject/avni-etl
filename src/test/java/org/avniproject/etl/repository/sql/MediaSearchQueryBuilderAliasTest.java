package org.avniproject.etl.repository.sql;

import org.avniproject.etl.builder.OrganisationIdentityBuilder;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.dto.ConceptFilterSearch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class MediaSearchQueryBuilderAliasTest {

    @BeforeEach
    public void setup() {
        OrgIdentityContextHolder.setContext(new OrganisationIdentityBuilder().withSchemaName("goonj").build());
    }

    @Test
    public void shouldGenerateCorrectTableAliasesForMultipleJoinsWithNonExactSearch() {
        ConceptFilterSearch filter1 = new ConceptFilterSearch("demand", "Target Community", 
            Arrays.asList("Mentally Challenged", "Adivasis"), null, null, false, true);

        ConceptFilterSearch filter2 = new ConceptFilterSearch("distribution", "Distribution Date", 
            null, "2025-09-14", "2025-09-17", false, true);

        ConceptFilterSearch filter3 = new ConceptFilterSearch("demand", "Number of people", 
            null, "2", "20", true, true);

        ConceptFilterSearch filter4 = new ConceptFilterSearch("dispatch_dispatch_receipt", "Dispatch Received Date", 
            null, "2025-09-01", "2025-09-19", false, true);

        ConceptFilterSearch filter5 = new ConceptFilterSearch("dispatch_dispatch_receipt", "Dispatch Status Id", 
            Arrays.asList("as12as"), null, null, false, false);

        List<ConceptFilterSearch> filters = Arrays.asList(filter1, filter2, filter3, filter4, filter5);

        Query query = new MediaSearchQueryBuilder()
                .withSearchConceptFilters(filters)
                .build();

        String sql = query.sql();
        System.out.println("Generated SQL:");
        System.out.println(sql);

        assertThat("Should create dispatch_dispatch_receipt_3 alias", sql, containsString("dispatch_dispatch_receipt_3"));
        assertThat("Should create dispatch_dispatch_receipt_4 alias", sql, containsString("dispatch_dispatch_receipt_4"));
        
        assertThat("Should reference dispatch_dispatch_receipt_4 in ILIKE condition", sql, containsString("dispatch_dispatch_receipt_4.\"Dispatch Status Id\" ILIKE"));
        assertThat("Should NOT reference dispatch_dispatch_receipt_0 in ILIKE condition", sql, not(containsString("dispatch_dispatch_receipt_0.\"Dispatch Status Id\" ILIKE")));
        
        assertThat("Should contain the ILIKE pattern", sql, containsString("ILIKE '%as12as%'"));
    }

    @Test
    public void shouldGenerateCorrectAliasesForSingleNonExactSearch() {
        ConceptFilterSearch filter = new ConceptFilterSearch("test_table", "test_column", 
            Arrays.asList("test_value"), null, null, false, false);

        Query query = new MediaSearchQueryBuilder()
                .withSearchConceptFilters(Arrays.asList(filter))
                .build();

        String sql = query.sql();
        
        assertThat("Should create test_table_0 alias", sql, containsString("test_table_0"));
        assertThat("Should reference test_table_0 in ILIKE condition", sql, containsString("test_table_0.\"test_column\" ILIKE"));
        assertThat("Should contain the ILIKE pattern", sql, containsString("ILIKE '%test_value%'"));
    }
}
