package org.avniproject.etl.service;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.avniproject.etl.domain.PostETLConfig;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks every active post-ETL config (a dir under post-etl/ holding post-etl-sync-processing-config.json;
 * a renamed .json_bkp is inactive) without a database. The service's ObjectMapper ignores unknown keys,
 * so a misspelt key silently skips its SQL; this test parses strictly instead.
 */
public class PostETLConfigFilesTest {
    private static final Path POST_ETL_DIR = Paths.get("src/main/resources/post-etl");
    private static final String CONFIG_FILE_NAME = "post-etl-sync-processing-config.json";
    private final ObjectMapper strictMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    @TestFactory
    Stream<DynamicTest> everyActiveConfigIsWellFormed() throws IOException {
        List<Path> configs;
        try (Stream<Path> dirs = Files.list(POST_ETL_DIR)) {
            configs = dirs.map(dir -> dir.resolve(CONFIG_FILE_NAME)).filter(Files::exists).sorted().collect(Collectors.toList());
        }
        assertFalse(configs.isEmpty(), "No active post-ETL config under " + POST_ETL_DIR.toAbsolutePath());
        return configs.stream().map(config -> DynamicTest.dynamicTest(config.getParent().getFileName().toString(), () -> check(config)));
    }

    @Test
    void sharedFileReferenceNormalisesOnTheClasspath() {
        // The service loads "post-etl/<schema>/<name>"; inside a jar ".." is not a directory step,
        // so ClassPathResource must clean the path itself for "../shared/x.sql" to load in production.
        assertEquals("post-etl/shared/assert-not-emptied.sql",
                new ClassPathResource("post-etl/jnpct/../shared/assert-not-emptied.sql").getPath());
    }

    private void check(Path configPath) throws IOException {
        Path dir = configPath.getParent();
        String schema = dir.getFileName().toString();
        PostETLConfig config = strictMapper.readValue(configPath.toFile(), PostETLConfig.class);
        List<String> failures = new ArrayList<>();
        Set<Path> referenced = new HashSet<>();
        Set<String> ddlTables = new TreeSet<>();
        Set<String> dmlTables = new TreeSet<>();

        for (PostETLConfig.DDLConfig ddl : nullSafe(config.getDdl())) {
            ddlTables.add(ddl.getTable());
            Path ddlFile = requireFile(dir, ddl.getSql(), "ddl " + ddl.getTable(), referenced, failures);
            if (ddlFile != null && !startsWithSetRole(Files.readString(ddlFile), schema)) {
                failures.add("ddl " + ddl.getTable() + ": " + ddl.getSql() + " must start with `set role " + schema
                        + ";`, or the table is owned by the ETL login and the org's own login cannot read it");
            }
            if (ddlFile != null) reportUnqualifiedCasts(Files.readString(ddlFile), "ddl " + ddl.getTable() + ": " + ddl.getSql(), failures);
            String existsCheck = ddl.getExistsCheck();
            if (existsCheck == null || !existsCheck.contains("table_schema = '" + schema + "'")) {
                failures.add("ddl " + ddl.getTable() + ": exists_check must name table_schema = '" + schema
                        + "'; the default check matches a same-named table or view in any schema");
            }
        }
        for (PostETLConfig.DMLConfig dml : nullSafe(config.getDml())) {
            dmlTables.add(dml.getTable());
            for (PostETLConfig.DMLSourceConfig source : nullSafe(dml.getSqls())) {
                List<String> files = new ArrayList<>();
                if (source.getInsertSql() != null && !source.getInsertSql().isBlank()) files.add(source.getInsertSql());
                files.addAll(nullSafe(source.getUpdateSqls()));
                if (files.isEmpty()) failures.add("dml " + dml.getTable() + "/" + source.getSourceTableName() + ": no SQL to run");
                for (String file : files) {
                    Path path = requireFile(dir, file, "dml " + dml.getTable(), referenced, failures);
                    if (path != null && Files.readString(path).contains(":param1") && nullSafe(source.getSqlParams()).isEmpty()) {
                        failures.add("dml " + dml.getTable() + ": " + file + " uses :param1 but its source has no sql-params");
                    }
                    if (path != null) reportUnqualifiedCasts(Files.readString(path), "dml " + dml.getTable() + ": " + file, failures);
                }
            }
        }
        if (!ddlTables.equals(dmlTables)) {
            failures.add("tables created by ddl " + ddlTables + " differ from tables refreshed by dml " + dmlTables);
        }
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(file -> file.toString().endsWith(".sql"))
                    .map(Path::normalize)
                    .filter(file -> !referenced.contains(file))
                    .forEach(file -> failures.add(dir.relativize(file) + " is in the config dir but no step runs it"));
        }
        assertTrue(failures.isEmpty(), schema + ":\n  " + String.join("\n  ", failures));
    }

    private Path requireFile(Path dir, String name, String owner, Set<Path> referenced, List<String> failures) {
        if (name == null || name.isBlank()) {
            failures.add(owner + ": names no SQL file");
            return null;
        }
        Path path = dir.resolve(name).normalize();
        referenced.add(path);
        if (!Files.isRegularFile(path)) {
            failures.add(owner + ": " + name + " does not exist");
            return null;
        }
        return path;
    }

    // Post-ETL sets search_path to the org schema alone, so only built-in types resolve unqualified.
    // A type from an extension in public (hstore, say) must be written public.<type>.
    private static final Set<String> BUILT_IN_TYPES = Set.of("text", "varchar", "char", "date", "time", "timestamp",
            "timestamptz", "interval", "integer", "int", "int2", "int4", "int8", "smallint", "bigint", "numeric",
            "decimal", "real", "float4", "float8", "boolean", "bool", "uuid", "json", "jsonb", "regclass", "oid");
    private static final Pattern CAST = Pattern.compile("::\\s*([A-Za-z_][A-Za-z0-9_]*)(\\s*\\.)?");

    private static void reportUnqualifiedCasts(String sql, String owner, List<String> failures) {
        Matcher cast = CAST.matcher(sql);
        while (cast.find()) {
            String type = cast.group(1).toLowerCase();
            boolean qualified = cast.group(2) != null;
            if (!qualified && !BUILT_IN_TYPES.contains(type)) {
                failures.add(owner + ": ::" + type + " must be schema-qualified (e.g. ::public." + type
                        + "); post-ETL's search_path holds only the org schema");
            }
        }
    }

    // The first statement, skipping blank lines and -- comments, must switch to the org's role
    private static boolean startsWithSetRole(String sql, String role) {
        for (String line : sql.split("\n")) {
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("--")) continue;
            return trimmed.toLowerCase().replace("\"", "").matches("set role " + Pattern.quote(role.toLowerCase()) + "\\s*;.*");
        }
        return false;
    }

    private static <T> List<T> nullSafe(List<T> list) {
        return list == null ? List.of() : list;
    }
}
