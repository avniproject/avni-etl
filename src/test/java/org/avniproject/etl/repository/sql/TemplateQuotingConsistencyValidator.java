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
 * Validates that SQL templates have consistent quoting for all schema and table references.
 * This prevents the systematic issue where INSERT INTO used quoted identifiers but FROM/JOIN used unquoted ones,
 * causing PostgreSQL to fail finding tables due to case sensitivity.
 * 
 * Can be disabled for CI/CD by setting environment variable:
 * SKIP_QUOTING_VALIDATION=true
 */
class TemplateQuotingConsistencyValidator {

    @Test
    @DisabledIfEnvironmentVariable(named = "SKIP_QUOTING_VALIDATION", matches = "true", disabledReason = "Quoting validation disabled for CI/CD")
    void validateConsistentQuotingAcrossAllTemplates() {
        Path sqlTemplateDir = Paths.get("src/main/resources/sql/etl");
        
        Stream<String> violations = findSqlTemplateFiles(sqlTemplateDir)
            .flatMap(this::checkTemplateForConsistentQuoting);
        
        String[] violationArray = violations.toArray(String[]::new);
        
        if (violationArray.length > 0) {
            String errorMessage = String.format(
                "Found %d SQL template quoting consistency violations:%n%s", 
                violationArray.length,
                String.join("%n", violationArray)
            );
            fail(errorMessage);
        }
    }

    private Stream<Path> findSqlTemplateFiles(Path directory) {
        try {
            return Files.walk(directory)
                .filter(path -> {
                    String fileName = path.toString();
                    return (fileName.endsWith(".sql.st") || fileName.endsWith(".sql") || fileName.endsWith(".sql.stg")) 
                        && Files.isRegularFile(path);
                });
        } catch (IOException e) {
            return Stream.empty();
        }
    }

    private Stream<String> checkTemplateForConsistentQuoting(Path templateFile) {
        try {
            String content = Files.readString(templateFile);
            String fileName = templateFile.getFileName().toString();
            
            // Skip certain template types that don't need schema quoting
            if (fileName.contains("function") || fileName.contains("procedure")) {
                return Stream.empty();
            }
            
            Stream<String> violations = Stream.empty();
            
            // Check for unquoted schema references in FROM/JOIN clauses with line numbers
            String[] lines = content.split("\n");
            for (int i = 0; i < lines.length; i++) {
                String line = lines[i];
                int lineNumber = i + 1;
                
                if (line.matches("(?s).*\\bfrom\\s+<schemaName>\\s*\\..*") || line.matches("(?s).*\\bFROM\\s+<schemaName>\\s*\\..*")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted schema reference in FROM clause: should use \"<schemaName>\"", fileName, lineNumber)
                    ));
                }
                
                if (line.matches("(?s).*\\binner join\\s+<schemaName>\\s*\\..*") || line.matches("(?s).*\\bINNER JOIN\\s+<schemaName>\\s*\\..*")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted schema reference in INNER JOIN clause: should use \"<schemaName>\"", fileName, lineNumber)
                    ));
                }
                
                if (line.matches("(?s).*\\bleft join\\s+<schemaName>\\s*\\..*") || line.matches("(?s).*\\bLEFT JOIN\\s+<schemaName>\\s*\\..*")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted schema reference in LEFT JOIN clause: should use \"<schemaName>\"", fileName, lineNumber)
                    ));
                }
                
                if (line.matches("(?s).*\\bright join\\s+<schemaName>\\s*\\..*") || line.matches("(?s).*\\bRIGHT JOIN\\s+<schemaName>\\s*\\..*")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted schema reference in RIGHT JOIN clause: should use \"<schemaName>\"", fileName, lineNumber)
                    ));
                }
                
                // Check for unquoted table references in FROM/JOIN clauses with line numbers
                if (line.matches("(?s).*\\bfrom\\s+<[^>]*>\\s*\\..*") && !line.contains("from \"<")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted table reference in FROM clause: should use \"<tableName>\"", fileName, lineNumber)
                    ));
                }
                
                if (line.matches("(?s).*\\bjoin\\s+<[^>]*>\\s*\\..*") && !line.contains("join \"<")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted table reference in JOIN clause: should use \"<tableName>\"", fileName, lineNumber)
                    ));
                }
                
                // Check for DELETE statements with unquoted schema/table with line numbers
                if (line.matches("(?s).*\\bdelete from\\s+<schemaName>\\s*\\..*") || line.matches("(?s).*\\bDELETE FROM\\s+<schemaName>\\s*\\..*")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted schema reference in DELETE FROM clause: should use \"<schemaName>\"", fileName, lineNumber)
                    ));
                }
                
                // Check for UPDATE statements with unquoted schema/table with line numbers
                if (line.matches("(?s).*\\bupdate\\s+<schemaName>\\s*\\..*") || line.matches("(?s).*\\bUPDATE\\s+<schemaName>\\s*\\..*")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted schema reference in UPDATE clause: should use \"<schemaName>\"", fileName, lineNumber)
                    ));
                }
                
                // Check for string replacement patterns with unquoted placeholders
                if (line.matches("(?s).*\\$\\{schema_name\\}\\s*\\..*") && !line.contains("\"\\${schema_name}\"")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted string replacement schema reference: should use \"\\${schema_name}\"", fileName, lineNumber)
                    ));
                }
                
                if (line.matches("(?s).*\\$\\{table_name\\}\\s*\\..*") && !line.contains("\"\\${table_name}\"")) {
                    violations = Stream.concat(violations, Stream.of(
                        String.format("Template %s:%d has unquoted string replacement table reference: should use \"\\${table_name}\"", fileName, lineNumber)
                    ));
                }
            }
            
            return violations;
            
        } catch (IOException e) {
            return Stream.of("Error reading template: " + templateFile.toString() + " - " + e.getMessage());
        }
    }
}
