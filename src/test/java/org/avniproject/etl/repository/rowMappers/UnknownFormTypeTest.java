package org.avniproject.etl.repository.rowMappers;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.repository.rowMappers.tableMappers.repeatableQuestionGroup.RepeatableQuestionGroupTableFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * avniproject/avni-etl#174, AC11 - a form type ETL does not recognise must not abort the organisation's
 * whole run.
 *
 * getFormTables() has no form-type filter, so every form mapping reaches the mapper. Three separate sites
 * used to throw on an unrecognised value, and each takes the entire org's ETL down rather than skipping
 * one table: Type.valueOf in getTableType, the default: in getTableStructure, and TableType.valueOf in
 * RepeatableQuestionGroupTableFactory - the last reached because repeatableQuestionGroups.sql selects
 * parent_table_type straight from f.form_type.
 *
 * This matters beyond forward compatibility. avni-etl and avni-server deploy minutes apart, and until
 * both are live an organisation that attaches one of the new forms has a form type the ETL has never
 * heard of. Skipping the table costs that org one stale table; throwing costs it all of its reporting.
 */
public class UnknownFormTypeTest {

    // Index names are built from the org's schema name off a thread-local, so a successful mapping needs
    // the context the ETL run would have set.
    @BeforeEach
    public void setUp() {
        OrgIdentityContextHolder.setContext(OrganisationIdentity.createForOrganisation("org174", "org174", "org174"));
    }

    @AfterEach
    public void tearDown() {
        OrgIdentityContextHolder.clear();
    }

    private List<Map<String, Object>> rowsFor(String formType) {
        Map<String, Object> row = new HashMap<>();
        row.put("table_type", formType);
        row.put("form_uuid", "form-uuid");
        row.put("subject_type_uuid", "subject-type-uuid");
        row.put("subject_type_name", "Mother");
        row.put("subject_type_type", "Individual");
        return List.of(row);
    }

    @Test
    public void skipsAFormTypeItDoesNotRecogniseInsteadOfThrowing() {
        assertThat("a form type from a newer server must be skipped, not fatal",
                new TableMetadataMapper().create(rowsFor("SomeFutureFormType")), nullValue());
    }

    @Test
    public void stillMapsTheFormTypesItDoesRecognise() {
        assertThat(new TableMetadataMapper().create(rowsFor("IndividualProfile")), notNullValue());
    }

    @Test
    public void mapsTheTwoNewTypesRatherThanSkippingThem() {
        assertThat(new TableMetadataMapper().create(rowsFor("Approval")), notNullValue());
        assertThat(new TableMetadataMapper().create(rowsFor("Rejection")), notNullValue());
    }

    /**
     * The third site. An approval form containing a repeatable question group reaches the factory with
     * parent_table_type = "Approval", which is not one of the four parent types a question group can hang
     * off. Repeatable question groups inside approval forms are deliberately out of scope for #174, so
     * the correct behaviour is no secondary table - not a dead organisation.
     */
    @Test
    public void skipsARepeatableQuestionGroupWhoseParentTypeIsNotSupported() {
        Map<String, Object> row = new HashMap<>();
        row.put("parent_table_type", "Approval");

        assertThat(RepeatableQuestionGroupTableFactory.create(row), nullValue());
    }

    @Test
    public void stillBuildsRepeatableQuestionGroupsForSupportedParents() {
        Map<String, Object> row = new HashMap<>();
        row.put("parent_table_type", "IndividualProfile");

        assertThat(RepeatableQuestionGroupTableFactory.create(row), notNullValue());
    }
}
