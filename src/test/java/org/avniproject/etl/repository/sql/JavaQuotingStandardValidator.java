package org.avniproject.etl.repository.sql;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
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

    // Patterns that indicate violations of the quoting standard
    private static final String[] VIOLATION_PATTERNS = {
        // wrapInQuotes calls when adding parameters to ST templates
        "wrapInQuotes.*\\.add\\(.*schemaName",
        "wrapInQuotes.*\\.add\\(.*tableName", 
        "\\.add\\(.*wrapInQuotes.*schema",
        "\\.add\\(.*wrapInQuotes.*table",
        // Direct string concatenation with quotes in template parameters
        "\\.add\\(.*\".*\".*schema",
        "\\.add\\(.*\".*\".*table"
    };

    // Skip patterns for legitimate usage (not violations)
    private static final String[] LEGITIMATE_USAGE_PATTERNS = {
        "SET search_path TO",  // Direct SQL execution
        "jdbcTemplate.execute", // Direct SQL execution
        "jdbcTemplate.update",  // Direct SQL execution
        "TransactionDataSyncHelper\\.java" // Skip the helper class itself
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
                "Found %d quoting standard violations:%n%s", 
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
            
            // Skip test files and the validator itself
            if (fileName.contains("Test") || fileName.equals("JavaQuotingStandardValidator.java")) {
                return Stream.empty();
            }
            
            // Skip TransactionDataSyncHelper itself (internal usage is legitimate)
            if (fileName.equals("TransactionDataSyncHelper.java")) {
                return Stream.empty();
            }

            // Skip files with legitimate SQL usage patterns
            for (String legitimatePattern : LEGITIMATE_USAGE_PATTERNS) {
                if (content.matches("(?s).*" + legitimatePattern + ".*")) {
                    return Stream.empty();
                }
            }

            // Check for violation patterns
            return Arrays.stream(VIOLATION_PATTERNS)
                .filter(pattern -> content.matches("(?s).*" + pattern + ".*"))
                .map(pattern -> String.format("File: %s - Found pattern: %s", 
                    javaFile.toString(), pattern));
                    
        } catch (IOException e) {
            return Stream.of("Error reading file: " + javaFile.toString() + " - " + e.getMessage());
        }
    }
}
