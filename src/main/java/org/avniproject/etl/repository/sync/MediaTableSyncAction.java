package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.result.SyncRegistrationConcept;
import org.avniproject.etl.repository.AvniMetadataRepository;
import org.avniproject.etl.repository.rowMappers.TableNameGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;
import static org.avniproject.etl.repository.sql.SqlFile.readSqlFile;

@Repository
public class MediaTableSyncAction implements EntitySyncAction {
    private final JdbcTemplate jdbcTemplate;
    private final AvniMetadataRepository avniMetadataRepository;
    private static final String mediaSql = readSqlFile("media.sql.st");
    private static final String mediaV2Sql = readSqlFile("mediaV2.sql.st");
    private static final String deleteDuplicateMediaSql = readSqlFile("deleteDuplicateMedia.sql.st");

    @Autowired
    public MediaTableSyncAction(JdbcTemplate jdbcTemplate, AvniMetadataRepository metadataRepository) {
        this.jdbcTemplate = jdbcTemplate;
        this.avniMetadataRepository = metadataRepository;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !tableMetadata.getType().equals(TableMetadata.Type.Media);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        if (this.doesntSupport(tableMetadata)) {
            return;
        }

        currentSchemaMetadata.getTableMetadata().forEach(thisTableMetadata -> {
            List<ColumnMetadata> mediaColumns = thisTableMetadata.findColumnsMatchingConceptType(ColumnMetadata.ConceptType.Image, ColumnMetadata.ConceptType.Video, ColumnMetadata.ConceptType.Audio, ColumnMetadata.ConceptType.File);
            mediaColumns.forEach(mediaColumn -> {
                insertData(tableMetadata, thisTableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime, false);
            });
            List<ColumnMetadata> mediaV2Columns = thisTableMetadata.findColumnsMatchingConceptType(ColumnMetadata.ConceptType.ImageV2);
            mediaV2Columns.forEach(mediaColumn -> {
                insertData(tableMetadata, thisTableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime, true);
            });
        });
        deleteDuplicateRows(lastSyncTime);
    }

    private void insertData(TableMetadata mediaTableMetadata, TableMetadata tableMetadata, ColumnMetadata mediaColumn, Date lastSyncTime, Date dataSyncBoundaryTime, boolean isV2) {
        syncNewerRows(mediaTableMetadata, tableMetadata, mediaColumn, lastSyncTime, dataSyncBoundaryTime, isV2);
    }

    private void syncNewerRows(TableMetadata mediaTableMetadata, TableMetadata tableMetadata, ColumnMetadata mediaColumn, Date lastSyncTime, Date dataSyncBoundaryTime, boolean isV2) {
        String fromTableName = tableMetadata.getName();
        String subjectTypeName = avniMetadataRepository.subjectTypeName(tableMetadata.getSubjectTypeUuid());
        String programName = avniMetadataRepository.programName(tableMetadata.getProgramUuid());
        String encounterTypeName = avniMetadataRepository.encounterTypeName(tableMetadata.getEncounterTypeUuid());
        String conceptName = avniMetadataRepository.conceptName(mediaColumn.getConceptUuid());
        String conceptColumnName = mediaColumn.getName();
        SyncRegistrationConcept[] syncRegistrationConcepts = avniMetadataRepository.findSyncRegistrationConcepts(tableMetadata.getSubjectTypeUuid());


        tableMetadata.getColumnMetadataList().forEach(columnMetadata -> {
            if (equalsButNotBothNull(columnMetadata.getConceptUuid(), syncRegistrationConcepts[0].getUuid())) {
                syncRegistrationConcepts[0].setColumnName(columnMetadata.getName());
            }

            if (equalsButNotBothNull(columnMetadata.getConceptUuid(), syncRegistrationConcepts[1].getUuid())) {
                syncRegistrationConcepts[1].setColumnName(columnMetadata.getName());
            }
        });

        ST template = new ST(isV2 ? mediaV2Sql : mediaSql)
                .add("schemaName", wrapInQuotes(OrgIdentityContextHolder.getDbSchema()))
                .add("tableName", wrapInQuotes(mediaTableMetadata.getName()))
                .add("conceptColumnName", wrapInQuotes(conceptColumnName))
                .add("subjectTypeName", wrapStringValue(subjectTypeName))
                .add("encounterTypeName", wrapStringValue(encounterTypeName))
                .add("programName", wrapStringValue(programName))
                .add("conceptName", wrapStringValue(conceptName))
                .add("fromTableName", wrapInQuotes(fromTableName))
                .add("startTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(lastSyncTime))
                .add("endTime", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(dataSyncBoundaryTime))
                .add("subjectTableName", subjectTypeTableName(subjectTypeName))
                .add("individualId", tableMetadata.isSubjectTable() ? "id" : "individual_id");
        if (syncRegistrationConcepts[0].getUuid() != null) {
            template = template
                    .add("syncRegistrationConcept1Name", wrapStringValue(syncRegistrationConcepts[0].getName()))
                    .add("syncRegistrationConcept1ColumnName", wrapInQuotes(syncRegistrationConcepts[0].getColumnName()));
        }
        if (syncRegistrationConcepts[1].getUuid() != null) {
            template = template
                    .add("syncRegistrationConcept2Name", wrapStringValue(syncRegistrationConcepts[1].getName()))
                    .add("syncRegistrationConcept2ColumnName", wrapInQuotes(syncRegistrationConcepts[1].getColumnName()));
        }
        if (tableMetadata.getType().equals(TableMetadata.Type.Person) && tableMetadata.hasColumn("middle_name")) {
            template = template
                .add("hasMiddleName", true);
        }

        String sql = template.render();

        runInOrgContext(() -> {
            jdbcTemplate.execute(sql);
            return NullObject.instance();
        }, jdbcTemplate);
    }

    private void deleteDuplicateRows(Date lastSyncTime) {
        String schema = OrgIdentityContextHolder.getDbSchema();
        String sql = new ST(deleteDuplicateMediaSql)
                .add("schemaName", schema)
                .render();
        HashMap<String, Object> params = new HashMap<>();
        params.put("lastSyncTime", lastSyncTime);

        runInOrgContext(() -> {
            new NamedParameterJdbcTemplate(jdbcTemplate).update(sql, params);
            return NullObject.instance();
        }, jdbcTemplate);
    }


    private String subjectTypeTableName(String subjectTypeName) {
        return new TableNameGenerator().generateName(List.of(subjectTypeName), "IndividualProfile", null);
    }

    private String wrapInQuotes(String parameter) {
        return parameter == null ? "null" : "\"" + parameter + "\"";

    }

    private String wrapStringValue(String parameter) {
        return parameter == null ? "null" : "'" + parameter.replace("'", "''") + "'";
    }

    public static boolean equalsButNotBothNull(Object a, Object b) {
        return a != null && b != null && a.equals(b);
    }
}
