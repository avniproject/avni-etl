package org.avniproject.etl.repository;

import org.avniproject.etl.domain.result.SyncRegistrationConcept;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Repository
public class AvniMetadataRepository {
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public AvniMetadataRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public String subjectTypeName(String subjectUuid) {
        return getNameFromUuid(subjectUuid, "subject_type");
    }

    public String programName(String programUuid) {
        return getNameFromUuid(programUuid, "program");
    }

    public String encounterTypeName(String encounterTypeUuid) {
        return getNameFromUuid(encounterTypeUuid, "encounter_type");
    }

    public String conceptName(String conceptUuid) {
        return getNameFromUuid(conceptUuid, "concept");
    }

    public SyncRegistrationConcept[] findSyncRegistrationConcepts(String subjectTypeUuid) {
        String sql = "select sync_registration_concept_1 as concept1_uuid, sync_registration_concept_2 as concept2_uuid from public.subject_type where uuid = :uuid limit 1";
        Map<String, Object> parameters = new HashMap<>(1);
        parameters.put("uuid", subjectTypeUuid);

        SyncRegistrationConcept[] syncRegistrationConcepts = runInOrgContext(() ->
                new NamedParameterJdbcTemplate(jdbcTemplate).queryForObject(sql, parameters,
                        (rs, rowNum) -> {
                            SyncRegistrationConcept[] concepts = new SyncRegistrationConcept[2];
                            SyncRegistrationConcept syncRegistrationConcept1 = new SyncRegistrationConcept();
                            syncRegistrationConcept1.setUuid(rs.getString("concept1_uuid"));
                            SyncRegistrationConcept syncRegistrationConcept2 = new SyncRegistrationConcept();
                            syncRegistrationConcept2.setUuid(rs.getString("concept2_uuid"));

                            concepts[0] = syncRegistrationConcept1;
                            concepts[1] = syncRegistrationConcept2;
                            return concepts;
                        }
                ), jdbcTemplate);

        Arrays.stream(syncRegistrationConcepts).forEach(this::fillInNameIfPossible);

        return syncRegistrationConcepts;
    }

    private void fillInNameIfPossible(SyncRegistrationConcept syncRegistrationConcept) {
        if (syncRegistrationConcept.getUuid() != null) {
            syncRegistrationConcept.setName(conceptName(syncRegistrationConcept.getUuid()));
        }
    }

    private String getNameFromUuid(String uuid, String tableName) {
        if (uuid == null || uuid.isEmpty()) {
            return null;
        }
        String sql = "select name from " + tableName + " where uuid = :uuid limit 1";
        Map<String, Object> parameters = new HashMap<>(1);
        parameters.put("uuid", uuid);

        return runInOrgContext(() -> new NamedParameterJdbcTemplate(jdbcTemplate).queryForObject(sql, parameters, String.class), jdbcTemplate);
    }
}
