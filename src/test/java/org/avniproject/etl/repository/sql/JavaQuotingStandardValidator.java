package org.avniproject.etl.repository.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates that Java code follows the standardized quoting approach:
 * - SQL templates handle quoting explicitly (e.g., "<schemaName>"."<tableName>")
 * - Java code passes unquoted values to StringTemplate templates
 * - No wrapInQuotes() calls when adding parameters to ST templates
 * 
 * Can be disabled for CI/CD by setting environment variable:
 * SKIP_QUOTING_VALIDATION=true
 */
class JavaQuotingStandardValidator {

    // Patterns that indicate violations of the quoting standard (focus on actual risks)
    private static final String[] VIOLATION_PATTERNS = {
        // wrapInQuotes calls when adding parameters to ST templates (HIGH RISK)
        "wrapInQuotes.*\\.add\\(.*schemaName",
        "wrapInQuotes.*\\.add\\(.*tableName", 
        "\\.add\\(.*wrapInQuotes.*schema",
        "\\.add\\(.*wrapInQuotes.*table",
        
        // String replacement with wrapInQuotes (HIGH RISK - causes double-quoting)
        "\\.replace.*wrapInQuotes.*schema",
        "\\.replace.*wrapInQuotes.*table",
        "wrapInQuotes.*\\.replace.*schema",
        "wrapInQuotes.*\\.replace.*table",
        
        // Direct string concatenation with quotes in ST template parameters (MEDIUM RISK)
        "\\.add\\([^,]*,\\s*\"[^\"]*schema[^\"]*\"\\)",
        "\\.add\\([^,]*,\\s*\"[^\"]*table[^\"]*\"\\)"
    };

    // Skip patterns for legitimate usage (not violations)
    private static final String[] LEGITIMATE_USAGE_PATTERNS = {
        "SET search_path TO",  // Direct SQL execution
        "jdbcTemplate.execute", // Direct SQL execution
        "jdbcTemplate.update",  // Direct SQL execution
        "TransactionDataSyncHelper\\.java", // Skip the helper class itself
        "\\.replace\\(\\\"\\$\\{schema_name\\}\\\"", // Legitimate string replacement (template has quotes)
        "\\.replace\\(\\\"\\$\\{table_name\\}\\\"",   // Legitimate string replacement (template has quotes)
        "\\.replace\\(\\\"\\$schemaName\\\"",        // Legitimate string replacement 
        "\\.replace\\(\\\"\\$tableName\\\"",         // Legitimate string replacement
        "ReportRepository\\.java" // Skip report repository (legitimate dynamic queries)
    };

    @Test
    @DisabledIfEnvironmentVariable(named = "SKIP_QUOTING_VALIDATION", matches = "true", disabledReason = "Quoting validation disabled for CI/CD")
    void validateNoWrapInQuotesInTemplateParameters() {
        Path javaSourceDir = Paths.get("src/main/java/org/avniproject/etl");
        
        Stream<String> violations = findJavaFiles(javaSourceDir)
            .flatMap(this::checkFileForViolations);
        
        String[] violationArray = violations.toArray(String[]::new);
        
        if (violationArray.length > 0) {
            String errorMessage = String.format(
                "Found %d Java quoting standard violations:%n%s", 
                violationArray.length,
                String.join("%n", violationArray)
            );
            fail(errorMessage);
        }
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "SKIP_QUOTING_VALIDATION", matches = "true", disabledReason = "Quoting validation disabled for CI/CD")
    void validateTransactionDataSyncHelperUnchanged() {
        Path helperFile = Paths.get("src/main/java/org/avniproject/etl/repository/sql/TransactionDataSyncHelper.java");
        
        try {
            String content = Files.readString(helperFile);
            
            // Ensure wrapInQuotes method has original implementation (no null handling)
            assertTrue(content.contains("public static String wrapInQuotes(String name) {"), 
                "wrapInQuotes method should exist");
            assertTrue(content.contains("return \"\\\"\" + name + \"\\\"\";"), 
                "wrapInQuotes should use original implementation without null handling");
            
            // Ensure no null handling that breaks database scripts
            assertFalse(content.contains("name == null ? \"null\" :"), 
                "wrapInQuotes should not return literal 'null' string");
                
        } catch (IOException e) {
            fail("Could not read TransactionDataSyncHelper.java: " + e.getMessage());
        }
    }

    private Stream<Path> findJavaFiles(Path directory) {
        try {
            return Files.walk(directory)
                .filter(path -> path.toString().endsWith(".java"))
                .filter(Files::isRegularFile);
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    private Stream<String> checkFileForViolations(Path javaFile) {
        try {
            String content = Files.readString(javaFile);
            String fileName = javaFile.getFileName().toString();
            String filePath = javaFile.toString();
            
            // Skip legitimate usage files
            for (String skipPattern : LEGITIMATE_USAGE_PATTERNS) {
                if (filePath.matches(".*" + skipPattern + ".*")) {
                    return Stream.empty();
                }
            }
            
            Stream<String> violations = Stream.empty();
            String[] lines = content.split("\n");
            
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i];
                int lineNumber = i + 1;
                
                for (String violationPattern : VIOLATION_PATTERNS) {
                    if (line.matches(".*" + violationPattern + ".*")) {
                        violations = Stream.concat(violations, Stream.of(
                            String.format("File %s:%d - %s: %s", fileName, lineNumber, violationPattern, line.trim())
                        ));
                    }
                }
            }
            
            return violations;
            
        } catch (IOException e) {
            return Stream.of("Error reading Java file: " + javaFile.toString() + " - " + e.getMessage());
        }
    }
}
