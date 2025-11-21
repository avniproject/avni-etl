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
                .filter(path -> path.toString().endsWith(".sql.st"))
                .filter(Files::isRegularFile);
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
            
            // Check for unquoted schema references in FROM/JOIN clauses
            if (content.contains("from <schemaName>.") || content.contains("FROM <schemaName>.")) {
                violations = Stream.concat(violations, Stream.of(
                    "Template " + fileName + " has unquoted schema reference in FROM clause: should use \"<schemaName>\""
                ));
            }
            
            if (content.contains("inner join <schemaName>.") || content.contains("INNER JOIN <schemaName>.")) {
                violations = Stream.concat(violations, Stream.of(
                    "Template " + fileName + " has unquoted schema reference in JOIN clause: should use \"<schemaName>\""
                ));
            }
            
            if (content.contains("left join <schemaName>.") || content.contains("LEFT JOIN <schemaName>.")) {
                violations = Stream.concat(violations, Stream.of(
                    "Template " + fileName + " has unquoted schema reference in LEFT JOIN clause: should use \"<schemaName>\""
                ));
            }
            
            if (content.contains("right join <schemaName>.") || content.contains("RIGHT JOIN <schemaName>.")) {
                violations = Stream.concat(violations, Stream.of(
                    "Template " + fileName + " has unquoted schema reference in RIGHT JOIN clause: should use \"<schemaName>\""
                ));
            }
            
            // Check for unquoted table references in FROM/JOIN clauses  
            if (content.matches("(?s).*from\\s+<[^>]*>\\s*\\.") && !content.contains("from \"<")) {
                violations = Stream.concat(violations, Stream.of(
                    "Template " + fileName + " has unquoted table reference in FROM clause: should use \"<tableName>\""
                ));
            }
            
            if (content.matches("(?s).*join\\s+<[^>]*>\\s*\\.") && !content.contains("join \"<")) {
                violations = Stream.concat(violations, Stream.of(
                    "Template " + fileName + " has unquoted table reference in JOIN clause: should use \"<tableName>\""
                ));
            }
            
            // Check for DELETE statements with unquoted schema/table
            if (content.contains("delete from <schemaName>.") || content.contains("DELETE FROM <schemaName>.")) {
                violations = Stream.concat(violations, Stream.of(
                    "Template " + fileName + " has unquoted schema reference in DELETE FROM clause: should use \"<schemaName>\""
                ));
            }
            
            // Check for UPDATE statements with unquoted schema/table
            if (content.contains("update <schemaName>.") || content.contains("UPDATE <schemaName>.")) {
                violations = Stream.concat(violations, Stream.of(
                    "Template " + fileName + " has unquoted schema reference in UPDATE clause: should use \"<schemaName>\""
                ));
            }
            
            return violations;
            
        } catch (IOException e) {
            return Stream.of("Error reading template: " + templateFile.toString() + " - " + e.getMessage());
        }
    }
}
