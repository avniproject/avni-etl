package org.avniproject.etl.repository.sql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.stringtemplate.v4.ST;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests for the mediaV3.sql.st template to verify correct SQL generation
 * for various media data formats (JSON object arrays, string arrays, plain URLs, etc).
 */
class MediaV3TemplateSqlTest {

    private String templateContent;

    @BeforeEach
    void setUp() {
        templateContent = SqlFile.readSqlFile("mediaV3.sql.st");
    }

    private String renderTemplate() {
        return renderTemplate(null, null);
    }

    private String renderTemplate(String syncConcept1ColName, String syncConcept2ColName) {
        ST template = new ST(templateContent)
                .add("schemaName", "test_schema")
                .add("tableName", "media")
                .add("conceptColumnName", "Photo")
                .add("questionGroupConceptName", "QG1")
                .add("subjectTypeName", "'Individual'")
                .add("encounterTypeName", "null")
                .add("programName", "null")
                .add("conceptName", "'Photo'")
                .add("fromTableName", "individual")
                .add("startTime", "2024-01-01T00:00:00.000")
                .add("endTime", "2024-12-31T23:59:59.999")
                .add("subjectTableName", "individual")
                .add("subjectIdColumnName", "id")
                .add("isRepeatable", false);

        if (syncConcept1ColName != null) {
            template.add("syncRegistrationConcept1Name", "'SyncKey1'");
            template.add("syncRegistrationConcept1ColumnName", syncConcept1ColName);
        }
        if (syncConcept2ColName != null) {
            template.add("syncRegistrationConcept2Name", "'SyncKey2'");
            template.add("syncRegistrationConcept2ColumnName", syncConcept2ColName);
        }

        return template.render();
    }

    @Test
    void shouldUseJsonArrayElementsNotText() {
        String sql = renderTemplate();
        assertThat(sql, containsString("json_array_elements("));
        assertFalse(sql.contains("json_array_elements_text("),
                "Should use json_array_elements, not json_array_elements_text");
    }

    @Test
    void shouldDetectJsonArraysByLeadingBracket() {
        String sql = renderTemplate();
        assertThat("Should check for leading [ to detect JSON arrays",
                sql, containsString("~ '^\\['"));
    }

    @Test
    void shouldCastJsonArrayDirectlyWithoutTextRoundTrip() {
        String sql = renderTemplate();
        assertThat("JSON arrays should be cast directly to json",
                sql, containsString("entity.\"Photo\"::json"));
        assertFalse(sql.contains("entity.\"Photo\"::text::json"),
                "Should not cast through text first");
    }

    @Test
    void shouldWrapNonArrayValuesAsJsonObjectWithUri() {
        String sql = renderTemplate();
        assertThat("Non-array values should be wrapped using json_build_object",
                sql, containsString("json_build_array(json_build_object('uri', COALESCE(entity.\"Photo\"::text, '')))"));
    }

    @Test
    void shouldExtractUriWithFallbackForStringElements() {
        String sql = renderTemplate();
        assertThat("Should use COALESCE to handle both object and string JSON elements",
                sql, containsString("COALESCE(media_json->>'uri', media_json#>>'{}')"));
    }

    @Test
    void shouldNotHaveRedundantJsonCastOnCaseEnd() {
        String sql = renderTemplate();
        assertFalse(sql.contains("END::json"),
                "CASE should not have redundant ::json cast since both branches already return json");
    }

    @Test
    void shouldCastMediaMetadataToJsonb() {
        String sql = renderTemplate();
        assertThat("media_metadata should be cast to jsonb for the jsonb column",
                sql, containsString("media_json::jsonb as media_metadata"));
    }

    @Test
    void shouldRenderWithoutSyncConcepts() {
        String sql = renderTemplate();
        // When no sync concepts, should have null placeholders
        assertThat(sql, containsString("null, null,"));
    }

    @Test
    void shouldRenderWithSyncConcepts() {
        String sql = renderTemplate("sync_col_1", "sync_col_2");
        assertThat(sql, containsString("'SyncKey1', entity.\"sync_col_1\""));
        assertThat(sql, containsString("'SyncKey2', entity.\"sync_col_2\""));
    }

    @Test
    void shouldRenderNonRepeatableIndex() {
        String sql = renderTemplate();
        assertThat(sql, containsString("-1 as repeatable_question_group_index"));
    }

    @Test
    void shouldRenderRepeatableIndex() {
        ST template = new ST(templateContent)
                .add("schemaName", "test_schema")
                .add("tableName", "media")
                .add("conceptColumnName", "Photo")
                .add("questionGroupConceptName", "QG1")
                .add("subjectTypeName", "'Individual'")
                .add("encounterTypeName", "null")
                .add("programName", "null")
                .add("conceptName", "'Photo'")
                .add("fromTableName", "individual")
                .add("startTime", "2024-01-01T00:00:00.000")
                .add("endTime", "2024-12-31T23:59:59.999")
                .add("subjectTableName", "individual")
                .add("subjectIdColumnName", "id")
                .add("isRepeatable", true);
        String sql = template.render();
        assertThat(sql, containsString("entity.repeatable_question_group_index"));
    }

    @Test
    void shouldHaveMiddleNameWhenFlagSet() {
        ST template = new ST(templateContent)
                .add("schemaName", "test_schema")
                .add("tableName", "media")
                .add("conceptColumnName", "Photo")
                .add("questionGroupConceptName", "QG1")
                .add("subjectTypeName", "'Individual'")
                .add("encounterTypeName", "null")
                .add("programName", "null")
                .add("conceptName", "'Photo'")
                .add("fromTableName", "individual")
                .add("startTime", "2024-01-01T00:00:00.000")
                .add("endTime", "2024-12-31T23:59:59.999")
                .add("subjectTableName", "individual")
                .add("subjectIdColumnName", "id")
                .add("isRepeatable", false)
                .add("hasMiddleName", true);
        String sql = template.render();
        assertThat(sql, containsString("subject.middle_name"));
    }

    @Test
    void shouldUseNullForMiddleNameWhenFlagNotSet() {
        String sql = renderTemplate();
        // Without hasMiddleName, should use null
        // Check that subject.middle_name is NOT in the output
        assertFalse(sql.contains("subject.middle_name"),
                "Should not reference subject.middle_name when hasMiddleName is not set");
    }

    @Test
    void shouldIncludeConceptNullFilter() {
        String sql = renderTemplate();
        assertThat("Should filter out null concept values",
                sql, containsString("entity.\"Photo\" is not null"));
    }

    @Test
    void shouldQuoteSchemaAndTableNames() {
        String sql = renderTemplate();
        assertThat(sql, containsString("\"test_schema\".\"media\""));
        assertThat(sql, containsString("\"test_schema\".\"individual\""));
    }

    @Test
    void shouldIncludeTimeRangeFilter() {
        String sql = renderTemplate();
        assertThat(sql, containsString("entity.\"last_modified_date_time\" > '2024-01-01T00:00:00.000'"));
        assertThat(sql, containsString("entity.\"last_modified_date_time\" <= '2024-12-31T23:59:59.999'"));
    }
}
