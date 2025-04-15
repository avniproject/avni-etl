package org.avniproject.etl;

import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.repository.ColumnMetadataRepository;
import org.avniproject.etl.service.EtlService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.jdbc.Sql;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

public class DataSyncIntegrationTest extends BaseIntegrationTest {
    @Autowired
    private EtlService etlService;
    @Autowired
    private ColumnMetadataRepository columnMetadataRepository;

    private static Date toDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

    private String getCurrentTime() {
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(toDate(LocalDateTime.now()));
    }

    private String getCurrentTime(long subtractSeconds) {
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(toDate(LocalDateTime.now().minus(Duration.ofSeconds(subtractSeconds))));
    }

    private void runDataSync() {
        etlService.runFor(OrganisationIdentity.createForOrganisation("orgc", "orgc", "orgc"));
    }

    private List<Map<String, Object>> getPersons() {
        return jdbcTemplate.queryForList("select * from orgc.person;");
    }

    private Map<String, Object> getPersonById(Integer id) {
        return jdbcTemplate.queryForMap(format("select * from orgc.person where id = %d", id));
    }

    private void addApprovalStatus(Integer id, String uuid, Integer statusId, String dateTime) {
        jdbcTemplate.execute(format("insert into entity_approval_status (id, uuid, entity_id, entity_type, approval_status_id, approval_status_comment,organisation_id, audit_id, version, status_date_time, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, entity_type_uuid, address_id, individual_id, sync_concept_1_value, sync_concept_2_value ) values (%d, '%s', 574170, 'Subject', %d, null, 12, create_audit(), 0,  '%s', 1,1, '%s', '%s', '19f9c741-a8b1-4be6-9aab-c0e5ae4e0cd8', 107786, 574170, null, null)", id, uuid, statusId, dateTime, dateTime, dateTime));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void addingNewFormElementAddsNewColumn() {
        runDataSync();
        assertThat(Objects.equals(getPersons().get(0).get("New concept"), new BigDecimal(123)), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void editingOlderRecordWillUpdateTheTable() {
        jdbcTemplate.execute("update individual set observations = observations || jsonb_build_object('01ee7e6b-6184-498d-bd54-dbe16e2f66dd', 456), last_modified_date_time = '2022-04-13 10:51:44.705 +00:00' where subject_type_id = 339;");
        runDataSync();
        assertThat(Objects.equals(getPersons().get(0).get("New concept"), new BigDecimal(456)), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void addingNewRecordWillAddOneRow() {
        runDataSync();
        assertThat(Objects.equals(getPersons().size(), 2), is(true));
        String newPersonQuery = format("INSERT INTO individual (id, uuid, address_id, observations, version, date_of_birth, date_of_birth_verified, gender_id, registration_date, organisation_id, first_name, last_name, is_voided, audit_id, facility_id, registration_location, subject_type_id, legacy_id, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time, sync_concept_1_value, sync_concept_2_value) VALUES (574172, '751bb8c8-ef18-4250-a73d-73106e7a5b66', 107786, '{}', 0, '2001-04-05', false, 389, '2022-04-05', 12, 'New', 'Person', false, create_audit(), null, null, 339, null, 1, 1, '%s', '%s', null, null);", getCurrentTime(), getCurrentTime());
        jdbcTemplate.execute(newPersonQuery);
        runDataSync();
        assertThat(Objects.equals(getPersons().size(), 3), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void changingConceptNameChangesColumnName() {
        jdbcTemplate.execute(format("update concept set name = 'Changed Concept', last_modified_date_time = '%s' where name = 'New concept'", getCurrentTime()));
        runDataSync();
        List<Map<String, Object>> persons = getPersons();
        assertThat(persons.get(0).containsKey("New concept"), is(false));
        assertThat(persons.get(0).containsKey("Changed Concept"), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void changingConceptAnswerShouldUpdateTheOldData() {
        runDataSync();
        Map<String, Object> person = getPersonById(574170);
        assertThat(Objects.equals(person.get("Single Select Question"), "Alpha"), is(true));
        jdbcTemplate.execute(format("update concept set name = 'alpha', last_modified_date_time = '%s' where name = 'Alpha'", getCurrentTime()));
        jdbcTemplate.execute(format("insert into answer_concept_migration (id, uuid, concept_id, old_answer_concept_name, new_answer_concept_name,organisation_id, version, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values (100, 'babf6656-c731-414a-96cd-30ebd00c6bfc', 107565, 'Alpha', 'alpha', 12, 0, 1, 1, '%s', '%s')", getCurrentTime(), getCurrentTime()));
        runDataSync();
        Map<String, Object> updatedPerson = getPersonById(574170);
        assertThat(Objects.equals(updatedPerson.get("Single Select Question"), "alpha"), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void changeDataTypeOfConceptIsNotSupported() {
        runDataSync();
        jdbcTemplate.execute(format("update concept set name = 'Beneficiary Children', data_type = 'Numeric', last_modified_date_time = '%s' where name = 'Beneficiary Children'", getCurrentTime()));
        assertThrows(RuntimeException.class, this::runDataSync);
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void changeDataTypeOfConcept_UsingVoiding_ButBetweenTwoRuns() {
        String conceptName = "Beneficiary Children";

        runDataSync();
        ColumnMetadata columnMetadata = this.columnMetadataRepository.findByName(conceptName);
        assertFalse(columnMetadata.isConceptVoided());

        String voidedConceptName = "Beneficiary Children (voided)";
        jdbcTemplate.execute(format("update concept set name = '%s', is_voided = true, last_modified_date_time = '%s' where name = '%s'",
                voidedConceptName, getCurrentTime(), conceptName));
        runDataSync();
        columnMetadata = this.columnMetadataRepository.findByUuid(columnMetadata.getConceptUuid());
        assertEquals(voidedConceptName, columnMetadata.getName());
        assertTrue(columnMetadata.isConceptVoided());

        String newConceptUuid = "81b7c2f2-3e43-4a4e-8614-5ccde27dea09";
        jdbcTemplate.execute(format("insert into concept (data_type, high_absolute, high_normal, low_absolute, low_normal, name, uuid, version, unit, organisation_id, is_voided, audit_id, key_values, active, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values ('Numeric', null, null, null, null, 'Beneficiary Children', '%s', 0, null, 12, false, create_audit(), null, true, 1, 1, '%s', '%s');", newConceptUuid, getCurrentTime(), getCurrentTime()));
        jdbcTemplate.execute(format("update form_element set concept_id = (select id from concept where name = '%s'), last_modified_date_time = '%s' where name = 'Beneficiary Children Form Element'", conceptName, getCurrentTime()));
        runDataSync();
        ColumnMetadata newColumnMetaData = this.columnMetadataRepository.findByUuid(newConceptUuid);
        assertFalse(newColumnMetaData.isConceptVoided());
        assertEquals(conceptName, newColumnMetaData.getName());
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void changeDataTypeOfConcept_UsingVoiding_ButBetweenTwoRuns_RepeatableQuestionGroup() {
        String conceptName = "Beneficiary Parent";

        runDataSync();
        ColumnMetadata columnMetadata = this.columnMetadataRepository.findByName(conceptName);
        assertFalse(columnMetadata.isConceptVoided());

        String voidedConceptName = "Beneficiary Parent (voided)";
        jdbcTemplate.execute(format("update concept set name = '%s', is_voided = true, last_modified_date_time = '%s' where name = '%s'",
                voidedConceptName, getCurrentTime(), conceptName));
        runDataSync();
        columnMetadata = this.columnMetadataRepository.findByUuid(columnMetadata.getConceptUuid());
        assertEquals(voidedConceptName, columnMetadata.getName());
        assertTrue(columnMetadata.isConceptVoided());

        String newConceptUuid = "a379fcef-f0a1-4ac2-8555-400041162fee";
        jdbcTemplate.execute(format("insert into concept (data_type, high_absolute, high_normal, low_absolute, low_normal, name, uuid, version, unit, organisation_id, is_voided, audit_id, key_values, active, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values ('Numeric', null, null, null, null, 'Beneficiary Parent', '%s', 0, null, 12, false, create_audit(), null, true, 1, 1, '%s', '%s');", newConceptUuid, getCurrentTime(), getCurrentTime()));
        jdbcTemplate.execute(format("update form_element set concept_id = (select id from concept where name = '%s'), last_modified_date_time = '%s' where name = 'Beneficiary Parent Form Element'", conceptName, getCurrentTime()));
        runDataSync();
        ColumnMetadata newColumnMetaData = this.columnMetadataRepository.findByUuid(newConceptUuid);
        assertFalse(newColumnMetaData.isConceptVoided());
        assertEquals(conceptName, newColumnMetaData.getName());
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void changeInDataTypeOfConcept_VoidOldAndRemoveFromForm_NewOneWithDifferentDataType() {
        String newConceptUuid = "81b7c2f2-3e43-4a4e-8614-5ccde27dea09";
        String conceptName = "Beneficiary Children";
        // Name voiding convention between avni main db and etl db are decoupled
        String voidedConceptName = "Beneficiary Children (voided)";

        runDataSync();
        // void the old concept
        jdbcTemplate.execute(format("update concept set name = '%s', is_voided = true, last_modified_date_time = '%s' where name = '%s'", voidedConceptName, getCurrentTime(), conceptName));
        // new concept with different data type
        jdbcTemplate.execute(format("""
                insert into concept (data_type, high_absolute, high_normal, low_absolute, low_normal, name, uuid, version, unit, organisation_id, is_voided, audit_id, key_values, active, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values ('Numeric', null, null, null, null, '%s', '%s', 0, null, 12, false, create_audit(), null, true, 1, 1, '%s', '%s');
                """, conceptName, newConceptUuid, getCurrentTime(), getCurrentTime()));
        // point form element to new concept
        jdbcTemplate.execute(format("""
                update form_element set concept_id = (select id from concept where name = '%s'), last_modified_date_time = '%s' where name = 'Beneficiary Children Form Element'
                """, conceptName, getCurrentTime()));
        runDataSync();

        ColumnMetadata oldColumnMetaData = this.columnMetadataRepository.findByName(ColumnMetadata.getVoidedName(conceptName));
        assertTrue(oldColumnMetaData.isConceptVoided());

        ColumnMetadata newColumnMetaData = this.columnMetadataRepository.findByUuid(newConceptUuid);
        assertFalse(newColumnMetaData.isConceptVoided());
        assertEquals(conceptName, newColumnMetaData.getName());
        // run again without any changes, should not get any error
        runDataSync();
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void changeInDataTypeOfConcept_VoidOldAndRemoveFromForm_NewOneWithDifferentDataType_LocationAttributes() {
        String newConceptUuid = "4a8103ba-92a6-4b66-8440-9d1296f38bd3";
        String conceptName = "Address Identifier";
        // Name voiding convention between avni main db and etl db are decoupled
        String voidedConceptName = "Address Identifier (voided)";

        runDataSync();
        // void the old concept
        jdbcTemplate.execute(format("update concept set name = '%s', is_voided = true, last_modified_date_time = '%s' where name = '%s'", voidedConceptName, getCurrentTime(), conceptName));
        // new concept with different data type
        jdbcTemplate.execute(format("""
                insert into concept (data_type, high_absolute, high_normal, low_absolute, low_normal, name, uuid, version, unit, organisation_id, is_voided, audit_id, key_values, active, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values ('Numeric', null, null, null, null, '%s', '%s', 0, null, 12, false, create_audit(), null, true, 1, 1, '%s', '%s');
                """, conceptName, newConceptUuid, getCurrentTime(), getCurrentTime()));
        // point form element to new concept
        jdbcTemplate.execute(format("""
                update form_element set concept_id = (select id from concept where name = '%s'), last_modified_date_time = '%s' where name = 'Address Identifier Form Element'
                """, conceptName, getCurrentTime()));
        runDataSync();

        ColumnMetadata oldColumnMetaData = this.columnMetadataRepository.findByName(ColumnMetadata.getVoidedName(conceptName));
        assertTrue(oldColumnMetaData.isConceptVoided());

        ColumnMetadata newColumnMetaData = this.columnMetadataRepository.findByUuid(newConceptUuid);
        assertFalse(newColumnMetaData.isConceptVoided());
        assertEquals(conceptName, newColumnMetaData.getName());
        // run again without any changes, should not get any error
        runDataSync();
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void multipleChangesOfConceptAnswersShouldBeRunOneAfterAnother() {
        jdbcTemplate.execute(format("update concept set name = 'secondChange', last_modified_date_time = '%s' where name = 'Alpha'", getCurrentTime()));
        jdbcTemplate.execute(format("insert into answer_concept_migration (id, uuid, concept_id, old_answer_concept_name, new_answer_concept_name,organisation_id, version, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values (100, 'babf6656-c731-414a-96cd-30ebd00c6bfd', 107565, 'Alpha', 'firstChange', 12, 0, 1, 1, '%s', '%s')", getCurrentTime(), getCurrentTime(1)));
        jdbcTemplate.execute(format("insert into answer_concept_migration (id, uuid, concept_id, old_answer_concept_name, new_answer_concept_name,organisation_id, version, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values (101, 'babf6656-c731-414a-96cd-30ebd00c6bfc', 107565, 'firstChange', 'secondChange', 12, 0, 1, 1, '%s', '%s')", getCurrentTime(), getCurrentTime()));
        runDataSync();
        Map<String, Object> updatedPerson = getPersonById(574170);
        assertThat(Objects.equals(updatedPerson.get("Single Select Question"), "secondChange"), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void changingConceptAnswerShouldUpdateTheOldMultiSelectData() {
        String conceptName = "Beneficiary Children";

        runDataSync();
        Map<String, Object> person = getPersonById(574170);
        assertThat(Objects.equals(person.get("Multi Select Question"), "Delta, Kappa"), is(true));
        jdbcTemplate.execute(format("update concept set name = 'kappa', last_modified_date_time = '%s' where name = 'Kappa'", getCurrentTime()));
        jdbcTemplate.execute(format("insert into answer_concept_migration (id, uuid, concept_id, old_answer_concept_name, new_answer_concept_name,organisation_id, version, created_by_id, last_modified_by_id, created_date_time, last_modified_date_time) values (101, 'babf6656-c731-414a-96cd-30ebd00c3bfc', 107569, 'Kappa', 'kappa', 12, 0, 1, 1, '%s', '%s')", getCurrentTime(), getCurrentTime()));
        runDataSync();
        Map<String, Object> updatedPerson = getPersonById(574170);
        assertThat(Objects.equals(updatedPerson.get("Multi Select Question"), "Delta, kappa"), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void latestApprovalStatusShouldGetAdded() {
        addApprovalStatus(100, "5f169db3-8c30-4647-ad60-181a4b4728f8", 1, "2022-04-05 11:47:04.749000");
        addApprovalStatus(101, "55195e01-9bcd-4492-a8d3-e59fff1e3a91", 2, "2022-04-05 12:47:04.749000");
        addApprovalStatus(102, "946ee24d-5b2e-4664-baca-e7477a245259", 3, "2022-04-05 13:47:04.749000");
        runDataSync();
        Map<String, Object> person = getPersonById(574170);
        assertThat(Objects.equals(person.get("latest_approval_status"), "Rejected"), is(true));
        addApprovalStatus(103, "5df91bc6-7e1a-42b8-9520-d420d2aadb39", 2, "2022-04-05 14:47:04.749000");
        runDataSync();
        Map<String, Object> newPerson = getPersonById(574170);
        assertThat(Objects.equals(newPerson.get("latest_approval_status"), "Approved"), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void groupFormElementGetsAddedAsColumns() {
        runDataSync();
        Map<String, Object> person = getPersonById(574170);
        assertThat(person.containsKey("Question group Child 1"), is(true));
        assertThat(person.containsKey("Question group Child 2"), is(true));
        assertThat(Objects.equals(person.get("Question group Child 1"), "value 1"), is(true));
        assertThat(Objects.equals(person.get("Question group Child 2"), "value 2"), is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/repeatableQuestionGroupObs-test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void repeatedQGObservationsArePopulatedInTheirOwnTable() {
        runDataSync();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(format("select * from orgc.person_general_encounter_asset_info where encounter_id = %d", 2001));
        assertEquals(2, list.size());
        assertEquals(100, ((BigDecimal) (list.get(0).get("Bitcoin"))).intValue());
        assertEquals("FTX", list.get(0).get("Exchange"));

        jdbcTemplate.execute("update encounter set last_modified_date_time = current_timestamp where id = 2001");
        runDataSync();
        assertEquals(2, list.size());
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void syncAttributeColumnShouldGetAdded() {
        runDataSync();
        Map<String, Object> programEncounter = jdbcTemplate.queryForMap("select * from orgc.person_nutrition_growth_monitoring where id = 877067");
        Map<String, Object> programEncounterCancel = jdbcTemplate.queryForMap("select * from orgc.person_nutrition_growth_monitoring_cancel where id = 877068");
        Map<String, Object> generalEncounter = jdbcTemplate.queryForMap("select * from orgc.person_general_encounter where id = 1900");
        Map<String, Object> generalEncounterCancel = jdbcTemplate.queryForMap("select * from orgc.person_general_encounter_cancel where id = 1901");
        Map<String, Object> enrolment = jdbcTemplate.queryForMap("select * from orgc.person_nutrition where id = 150708");
        Map<String, Object> exit = jdbcTemplate.queryForMap("select * from orgc.person_nutrition_exit where id = 150709");
        Arrays.asList(programEncounter, programEncounterCancel, generalEncounter, generalEncounterCancel, enrolment, exit).forEach(entity -> {
            assertThat(entity.containsKey("Text Question"), is(true));
            assertThat(Objects.equals(entity.get("Text Question"), "This is a text"), is(true));
        });
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void addressLevelColumnsAreCreated() {
        runDataSync();
        List<Map<String, Object>> addresses = jdbcTemplate.queryForList("select * from orgc.address;");
        Map<String, Object> address = addresses.get(0);
        assertThat(address.containsKey("District"), is(true));
        assertThat(address.containsKey("Block"), is(true));
        assertThat(address.containsKey("Gram Panchayat"), is(true));
        assertThat(addresses.size() == 6, is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/organisation-group-teardown.sql", "/organisation-group.sql"})
    @Sql(scripts = {"/test-data-teardown.sql", "/organisation-group-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void allTheDBUserOfOrgGroupAreAbleToQueryTables() {
        etlService.runForOrganisationGroup("og");
        etlService.runForOrganisationGroup("og");

        etlService.runFor(OrganisationIdentity.createForOrganisation("ogi1", "ogi1", "ogi1"));

        etlService.runForOrganisationGroup("og");
        etlService.runFor(OrganisationIdentity.createForOrganisation("ogi1", "ogi1", "ogi1"));

        jdbcTemplate.execute("set role og;");
        List<Map<String, Object>> groupList = jdbcTemplate.queryForList("select * from og.person;");
        jdbcTemplate.execute("set role ogi1;");
        List<Map<String, Object>> child1List = jdbcTemplate.queryForList("select * from og.person;");
        jdbcTemplate.execute("set role ogi2;");
        List<Map<String, Object>> child2List = jdbcTemplate.queryForList("select * from og.person;");
        jdbcTemplate.execute("reset role;");

        assertThat("Group organisation schema contains data from both individual organisations", groupList.size(), is(2));
        assertThat("Member organisations will also get the same data as the group organisations", child1List.size(), is(2));
        assertThat("Member organisations will also get the same data as the group organisations", child2List.size(), is(2));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/organisation-group-teardown.sql", "/organisation-group.sql"})
    @Sql(scripts = {"/test-data-teardown.sql", "/organisation-group-teardown.sql"}, executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void multipleRunsShouldNotCauseDuplicateDataInOrganisationsAndGroups() {

        etlService.runFor(OrganisationIdentity.createForOrganisation("ogi2", "ogi2", "ogi2"));
        etlService.runForOrganisationGroup("og");
        etlService.runFor(OrganisationIdentity.createForOrganisation("ogi1", "ogi1", "ogi2"));

        etlService.runFor(OrganisationIdentity.createForOrganisation("ogi2", "ogi2", "ogi2"));
        etlService.runForOrganisationGroup("og");
        etlService.runFor(OrganisationIdentity.createForOrganisation("ogi1", "ogi1", "ogi2"));

        jdbcTemplate.execute("set role og;");
        List<Map<String, Object>> groupList = jdbcTemplate.queryForList("select * from og.person;");
        jdbcTemplate.execute("set role ogi1;");
        List<Map<String, Object>> child1List = jdbcTemplate.queryForList("select * from ogi1.person;");
        jdbcTemplate.execute("set role ogi2;");
        List<Map<String, Object>> child2List = jdbcTemplate.queryForList("select * from ogi2.person;");
        jdbcTemplate.execute("reset role;");

        assertThat(groupList.size(), is(2));
        assertThat(child1List.size(), is(1));
        assertThat(child2List.size(), is(1));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/organisation-group.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void approvalStatusFromChildOrgsShouldUpdateCorrectly() {
        etlService.runForOrganisationGroup("og");
        Map<String, Object> org1Person = jdbcTemplate.queryForMap("select * from og.person where id = 674170;");
        Map<String, Object> org2Person = jdbcTemplate.queryForMap("select * from og.person where id = 574170;");
        assertThat(org1Person.get("latest_approval_status"), is("Pending"));
        assertThat(org2Person.get("latest_approval_status"), is("Approved"));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/new-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void allTablesHaveAddressIdAndIndividualIdColumns() {
        runDataSync();
        List<Map<String, Object>> enrolments = jdbcTemplate.queryForList("select * from orgc.person_nutrition;");
        assertThat(enrolments.size() == 3, is(true));
        List<Map<String, Object>> exits = jdbcTemplate.queryForList("select * from orgc.person_nutrition_exit;");
        assertThat(exits.size() == 2, is(true));
        List<Map<String, Object>> programEncounters = jdbcTemplate.queryForList("select * from orgc.person_nutrition_growth_monitoring;");
        assertThat(programEncounters.size() == 3, is(true));
        List<Map<String, Object>> programEncounterCancels = jdbcTemplate.queryForList("select * from orgc.person_nutrition_growth_monitoring_cancel;");
        assertThat(programEncounterCancels.size() == 1, is(true));
        List<Map<String, Object>> encounters = jdbcTemplate.queryForList("select * from orgc.person_general_encounter;");
        assertThat(encounters.size() == 3, is(true));
        List<Map<String, Object>> encounterCancels = jdbcTemplate.queryForList("select * from orgc.person_general_encounter_cancel;");
        assertThat(encounterCancels.size() == 1, is(true));
        Arrays.asList(enrolments, programEncounters, encounters, exits, programEncounterCancels, encounterCancels).forEach(entity -> {
            assertThat(entity.get(0).containsKey("individual_id"), is(true));
            assertThat(entity.get(0).containsKey("address_id"), is(true));
        });
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/media-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldPopulateMediaTable() {
        runDataSync();
        List<Map<String, Object>> media = jdbcTemplate.queryForList("select * from orgc.media;");
        assertThat(media.size(), is(5));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/media-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void shouldPopulateMediaTableCorrectlyWhenTransactionalDataUpdates() throws InterruptedException {
        runDataSync();
        List<Map<String, Object>> media = jdbcTemplate.queryForList("select * from orgc.media;");
        assertThat("Verifying current value of media table", media.size(), is(5));

        String newEncounterImage = "https://s3.amazon.com/newEncounterImage.jpg";
        jdbcTemplate.execute("update encounter set observations = observations || jsonb_build_object('44163589-f76d-447d-9b6e-f5c32aa859eb', '" + newEncounterImage + "'), last_modified_date_time = now() where id = 1900;");
        runDataSync();

        media = jdbcTemplate.queryForList("select * from orgc.media;");
        assertThat("Media table number of rows has not changed since last run", media.size(), is(5));

        Optional<Map<String, Object>> encounterMedia = media.stream().filter(stringObjectMap -> (Integer.valueOf(1900)).equals(stringObjectMap.get("entity_id"))).findAny();
        assertThat(encounterMedia.isPresent(), is(true));
        assertThat(encounterMedia.get().get("image_url"), is(newEncounterImage));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql", "/media-form-element.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void arrayStyleMediaObservationsCreateMultipleRowsInMediaTable() throws InterruptedException {
        String encounterImages = "[\"https://s3.amazon.com/image1.jpg\", \"https://s3.amazon.com/image2.jpg\"]";
        String sql = "update encounter set last_modified_date_time = now(), observations = observations || '{\"44163589-f76d-447d-9b6e-f5c32aa859eb\": " + encounterImages + "}'::jsonb where id = 1900;";
        jdbcTemplate.execute(sql);

        runDataSync();

        List<Map<String, Object>> media = jdbcTemplate.queryForList("select * from orgc.media;");
        assertThat("Media table number of rows has not changed since last run", media.size(), is(6));

        String encounterImagesSecondSet = "[\"https://s3.amazon.com/image3.jpg\", \"https://s3.amazon.com/image4.jpg\", \"https://s3.amazon.com/image5.jpg\"]";
        String sqlSecondItr = "update encounter set last_modified_date_time = now(), observations = observations || '{\"44163589-f76d-447d-9b6e-f5c32aa859eb\": " + encounterImagesSecondSet + "}'::jsonb where id = 1900;";
        jdbcTemplate.execute(sqlSecondItr);

        runDataSync();

        List<Map<String, Object>> mediaSecondItr = jdbcTemplate.queryForList("select * from orgc.media;");
        assertThat("Media table number of rows was supposed to increase since last run", mediaSecondItr.size(), is(7));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void userTableShouldUpdateWithOldUserData() throws InterruptedException {
        String numberOfUserRows = "select count(*) from orgc.users where id = 3453;";

        runDataSync();
        Long numberOfRowsAfterFirstRun = countOfRowsIn("orgc.users");

        String updateLastModifiedDateTimeSql = "update public.users set last_modified_date_time = now() where id = 3453;";
        jdbcTemplate.execute(updateLastModifiedDateTimeSql);

        runDataSync();

        Long numberOfRowsAfterSecondRun = countOfRowsIn("orgc.users");
        assertThat(numberOfRowsAfterSecondRun, is(equalTo(numberOfRowsAfterFirstRun)));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void _userTableShouldUpdateWithIsVoided() {
        runDataSync();
        String updateIsVoided = "update public.users set last_modified_date_time = now(),is_voided = true where id = 3453;";
        jdbcTemplate.execute(updateIsVoided);

        runDataSync();
        List<Map<String, Object>> list = jdbcTemplate.queryForList("select * from orgc.users where is_voided = true and id = 3453 ; ");
        assertThat(list.size(), is(equalTo(1)));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void cancelDateTimeShouldBeUpdatedWhenProgramEncounterIsCancelled() throws InterruptedException {
        runDataSync();

        List<Map<String, Object>> programEncounterToBeCancelled = jdbcTemplate.queryForList("select * from orgc.person_nutrition_growth_monitoring where id = 877069;");
        assertThat(programEncounterToBeCancelled.size() == 1, is(true));
        assertThat(programEncounterToBeCancelled.get(0).get("cancel_date_time") == null, is(true));

        jdbcTemplate.execute(format("update program_encounter set  last_modified_date_time = '%s', cancel_date_time = now() where id = 877069;", getCurrentTime()));

        runDataSync();

        List<Map<String, Object>> cancelledProgramEncounter = jdbcTemplate.queryForList("select * from orgc.person_nutrition_growth_monitoring where id = 877069;");
        assertThat(cancelledProgramEncounter.get(0).get("cancel_date_time") == null, is(false));

        List<Map<String, Object>> valueInCancelTable = jdbcTemplate.queryForList("select * from orgc.person_nutrition_growth_monitoring_cancel where id = 877069;");
        assertThat(programEncounterToBeCancelled.size() == 1, is(true));
    }

    @Test
    @Sql({"/test-data-teardown.sql", "/test-data.sql"})
    @Sql(scripts = "/test-data-teardown.sql", executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD)
    public void cancelDateTimeShouldBeUpdatedWhenGeneralEncounterIsCancelled() throws InterruptedException {
        runDataSync();

        List<Map<String, Object>> programEncounterToBeCancelled = jdbcTemplate.queryForList("select * from orgc.person_general_encounter where id = 1902;");
        assertThat(programEncounterToBeCancelled.size() == 1, is(true));
        assertThat(programEncounterToBeCancelled.get(0).get("cancel_date_time") == null, is(true));

        jdbcTemplate.execute(format("update encounter set  last_modified_date_time = '%s', cancel_date_time = now() where id = 1902;", getCurrentTime()));

        runDataSync();

        List<Map<String, Object>> cancelledProgramEncounter = jdbcTemplate.queryForList("select * from orgc.person_general_encounter where id = 1902;");
        assertThat(cancelledProgramEncounter.get(0).get("cancel_date_time") == null, is(false));

        List<Map<String, Object>> valueInCancelTable = jdbcTemplate.queryForList("select * from orgc.person_general_encounter_cancel where id = 1902;");
        assertThat(programEncounterToBeCancelled.size() == 1, is(true));
    }
}

