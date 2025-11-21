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
        
        Stream<String> violations = findAllSqlTemplateFiles(sqlTemplateDir)
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

    private Stream<Path> findAllSqlTemplateFiles(Path directory) {
        try {
            return Files.walk(directory)
                .filter(path -> {
                    String fileName = path.toString();
                    return (fileName.endsWith(".sql.st") || fileName.endsWith(".sql")) 
                        && Files.isRegularFile(path);
                });
        } catch (IOException e) {
            return Stream.empty();
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

    
    private Stream<String> checkTemplateForQuotingViolations(Path templateFile) {
        try {
            String content = Files.readString(templateFile);
            String fileName = templateFile.getFileName().toString();
            
            // Skip certain template types that don't need schema quoting
            if (fileName.contains("function") || fileName.contains("procedure")) {
                return Stream.empty();
            }
            
            Stream<String> violations = Stream.empty();
            String[] lines = content.split("\n");
            
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i];
                int lineNumber = i + 1;
                
                // Check for unquoted schemaName references
                if (line.contains("<schemaName>") && !line.matches(".*\"<schemaName>\".*")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted schemaName reference: %s", fileName, lineNumber, line.trim())
                    ));
                }
                
                // Check for unquoted tableName references
                if (line.contains("<tableName>") && !line.matches(".*\"<tableName>\".*")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted tableName reference: %s", fileName, lineNumber, line.trim())
                    ));
                }
                
                // Check for unquoted table parameter references (comprehensive)
                String[] tableParams = {"<fromTableName>", "<subjectTableName>", "<parentTableName>", "<mediaAnalysisTable>", "<encounterCancelTableName>", "<primaryTableName>", "<exitTableName>"};
                for (String param : tableParams) {
                    if (line.contains(param) && !line.matches(".*\"" + param.replace("<", "\\<").replace(">", "\\>") + "\".*")) {
                        violations = Stream.concat(violations, Stream.of(
                            String.format("Template %s:%d has unquoted %s reference: %s", fileName, lineNumber, param, line.trim())
                        ));
                    }
                }
                
                // Check for unquoted string replacement patterns
                if (line.contains("${schema_name}") && !line.contains("\"${schema_name}\"")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted ${schema_name} reference: %s", fileName, lineNumber, line.trim())
                    ));
                }
                
                if (line.contains("${table_name}") && !line.contains("\"${table_name}\"")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted ${table_name} reference: %s", fileName, lineNumber, line.trim())
                    ));
                }
            }
            
            return violations;
            
        } catch (IOException e) {
            return Stream.of("Error reading template: " + templateFile.toString() + " - " + e.getMessage());
        }
    }
}
