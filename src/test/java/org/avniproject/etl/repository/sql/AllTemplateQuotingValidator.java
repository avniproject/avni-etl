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
 * Validates that all SQL templates follow the standardized quoting approach:
 * - Schema and table names are explicitly quoted in templates
 * - Uses proper PostgreSQL quoting: "<schemaName>"."<tableName>"
 * - No unquoted schema/table references in templates
 * 
 * Can be disabled for CI/CD by setting environment variable:
 * SKIP_QUOTING_VALIDATION=true
 */
class AllTemplateQuotingValidator {

    @Test
    @DisabledIfEnvironmentVariable(named = "SKIP_QUOTING_VALIDATION", matches = "true", disabledReason = "Quoting validation disabled for CI/CD")
    void validateAllSqlTemplatesHaveExplicitQuoting() {
        Path sqlTemplateDir = Paths.get("src/main/resources/sql/etl");
        
        Stream<String> violations = findSqlTemplateFiles(sqlTemplateDir)
            .flatMap(this::checkTemplateForQuotingViolations);
        
        String[] violationArray = violations.toArray(String[]::new);
        
        if (violationArray.length > 0) {
            String errorMessage = String.format(
                "Found %d SQL template quoting violations:%n%s", 
                violationArray.length,
                String.join("%n", violationArray)
            );
            fail(errorMessage);
        }
    }

    @Test
    @DisabledIfEnvironmentVariable(named = "SKIP_QUOTING_VALIDATION", matches = "true", disabledReason = "Quoting validation disabled for CI/CD")
    void validateKeyTemplatesHaveProperQuoting() {
        String[] keyTemplates = {
            "user.sql.st",
            "media.sql.st", 
            "mediaAnalysis.sql.st",
            "deleteUncancelledEncounters.sql.st",
            "deleteInvalidExits.sql.st",
            "syncTelemetry.sql.st"
        };
        
        for (String templateName : keyTemplates) {
            Path templatePath = Paths.get("src/main/resources/sql/etl/" + templateName);
            
            if (Files.exists(templatePath)) {
                try {
                    String content = Files.readString(templatePath);
                    
                    // Check for proper quoting patterns
                    boolean hasProperQuoting = content.matches("(?s).*\"<schemaName>\".*") && 
                                             (content.matches("(?s).*\"<tableName>\".*") || 
                                              content.matches("(?s).*\"<mediaAnalysisTable>\".*") ||
                                              content.matches("(?s).*\"<encounterCancelTableName>\".*") ||
                                              content.matches("(?s).*\"<primaryTableName>\".*") ||
                                              content.matches("(?s).*\"<exitTableName>\".*"));
                    
                    if (!hasProperQuoting) {
                        fail("Template " + templateName + " should have explicit quoting for schema and table parameters");
                    }
                    
                } catch (IOException e) {
                    fail("Could not read template " + templateName + ": " + e.getMessage());
                }
            }
        }
    }

    private Stream<Path> findSqlTemplateFiles(Path directory) {
        try {
            return Files.walk(directory)
                .filter(path -> path.toString().endsWith(".sql.st"))
                .filter(Files::isRegularFile);
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    private Stream<String> checkTemplateForQuotingViolations(Path templateFile) {
        try {
            String content = Files.readString(templateFile);
            String fileName = templateFile.getFileName().toString();
            
            // Skip certain template types that don't need schema quoting
            if (fileName.contains("function") || fileName.contains("procedure")) {
                return Stream.empty();
            }
            
            // Check for unquoted schema/table references
            if (content.contains("<schemaName>") && !content.matches("(?s).*\"<schemaName>\".*")) {
                return Stream.of("Template " + fileName + " has unquoted schemaName reference");
            }
            
            if (content.contains("<tableName>") && !content.matches("(?s).*\"<tableName>\".*")) {
                return Stream.of("Template " + fileName + " has unquoted tableName reference");
            }
            
            // Check for other common table references that might need quoting
            String[] commonTableRefs = {"individual", "encounter", "program_enrolment", "program_encounter"};
            for (String tableRef : commonTableRefs) {
                if (content.contains(tableRef) && !content.matches("(?s).*\"" + tableRef + "\".*")) {
                    // Only flag if it looks like a table reference (not a column name) and not in public schema
                    if ((content.matches("(?s).*\\b" + tableRef + "\\b.*FROM.*") || 
                        content.matches("(?s).*\\b" + tableRef + "\\b.*JOIN.*") ||
                        content.matches("(?s).*\\b" + tableRef + "\\b.*INSERT INTO.*") ||
                        content.matches("(?s).*\\b" + tableRef + "\\b.*CREATE TABLE.*")) &&
                        !content.contains("public." + tableRef)) {
                        return Stream.of("Template " + fileName + " has potentially unquoted table reference: " + tableRef);
                    }
                }
            }
            
            return Stream.empty();
            
        } catch (IOException e) {
            return Stream.of("Error reading template: " + templateFile.toString() + " - " + e.getMessage());
        }
    }
}
