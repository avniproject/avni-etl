package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.NullObject;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.repository.sql.SqlFile;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.stringtemplate.v4.ST;

import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import static org.avniproject.etl.repository.JdbcContextWrapper.runInOrgContext;

@Repository
public class CleanEncounterCancelTableAction implements EntitySyncAction {
    private final JdbcTemplate jdbcTemplate;
    private static final String deleteUncancelledEncountersSqlTemplate = SqlFile.readSqlFile("deleteUncancelledEncounters.sql.st");
    @Autowired
    public CleanEncounterCancelTableAction(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        if (this.doesntSupport(tableMetadata)) {
            return;
        }
        cleanUncancelledEncounters(tableMetadata, currentSchemaMetadata);
    }

    private void cleanUncancelledEncounters(TableMetadata tableMetadata, SchemaMetadata currentSchemaMetadata) {
        String schema = OrgIdentityContextHolder.getDbSchema();
        String encounterCancelTableName = tableMetadata.getName();
        String primaryTableName = getPrimaryTableName(tableMetadata, currentSchemaMetadata);
        String sql = new ST(deleteUncancelledEncountersSqlTemplate)
                .add("schemaName", wrapInQuotes(schema))
                .add("encounterCancelTableName", wrapInQuotes(encounterCancelTableName))
                .add("primaryTableName", wrapInQuotes(primaryTableName))
                .render();
        runInOrgContext(() -> {
            jdbcTemplate.execute(sql);
            return NullObject.instance();
        }, jdbcTemplate);
    }

    private String getPrimaryTableName(TableMetadata tableMetadata, SchemaMetadata currentSchemaMetadata) {
        Optional<TableMetadata> primaryTableMetadata = currentSchemaMetadata.getAllIndividualAndProgramEncounterTables()
                .stream().filter(tm -> tm.getEncounterTypeUuid().equals(tableMetadata.getEncounterTypeUuid())
                        && (Objects.isNull(tm.getProgramUuid()) || (Objects.nonNull(tableMetadata.getProgramUuid()) && tableMetadata.getProgramUuid().equals(tm.getProgramUuid())))
                        && tm.getSubjectTypeUuid().equals(tableMetadata.getSubjectTypeUuid())).findAny();

        if (primaryTableMetadata.isPresent()) {
            return primaryTableMetadata.get().getName();
        }
        throw new RuntimeException(String.format("Corresponding Primary table not found for cancel table: %s", tableMetadata.getName()));
    }

    private String wrapInQuotes(String parameter) {
        return parameter == null ? "null" : "\"" + parameter + "\"";
    }

    private boolean supports(TableMetadata tableMetadata) {
        return Arrays.asList(TableMetadata.Type.IndividualEncounterCancellation, TableMetadata.Type.ProgramEncounterCancellation).contains(tableMetadata.getType());
    }

    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !supports(tableMetadata);
    }

}
