package org.avniproject.etl.domain.metadata;

import org.junit.jupiter.api.Test;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.domain.metadata.diff.RenameColumn;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression test that FAILS if the column truncation bug is reintroduced.
 * This test ensures that TableMetadata.findChanges() always produces
 * properly truncated voided column names.
 */
class ColumnTruncationRegressionTest {

    @Test
    void tableMetadataMustCreateProperlyTruncatedVoidedColumns() {
        // Set up context - ensure it's not a group organisation
        OrgIdentityContextHolder.setContext(
            OrganisationIdentity.createForOrganisation("test_user", "test_schema", "test")
        );

        // Create the existing table with a column that has a long name
        TableMetadata existingTable = new TableMetadata();
        existingTable.setName("test_table");
        
        // Use a name that when voided would exceed 63 chars without proper truncation
        String longName = "Please provide name of child and information about suffering from some very long disease name";
        Column column = new Column(longName, Column.Type.text);
        ColumnMetadata columnMetadata = new ColumnMetadata(
            column, 1, ColumnMetadata.ConceptType.Text, "concept-uuid", false
        );
        existingTable.addColumnMetadata(List.of(columnMetadata));

        // New table without the column (simulating form element removal)
        TableMetadata newTable = new TableMetadata();
        newTable.setName("test_table");
        // IMPORTANT: Don't add any columns to new table to simulate removal

        // Get the changes - this should create a voided column
        // IMPORTANT: Call findChanges on newTable comparing with existingTable
        List<org.avniproject.etl.domain.metadata.diff.Diff> changes = newTable.findChanges(existingTable);
        
        // Debug: Check what changes we got
        System.out.println("Number of changes: " + changes.size());
        changes.forEach(change -> {
            System.out.println("Change type: " + change.getClass().getSimpleName());
            if (change instanceof RenameColumn) {
                RenameColumn rc = (RenameColumn) change;
                System.out.println("  Rename SQL: " + rc.getSql());
            }
        });
        
        // Look for the rename change that creates the voided column
        RenameColumn renameChange = changes.stream()
            .filter(RenameColumn.class::isInstance)
            .map(RenameColumn.class::cast)
            .filter(rc -> rc.getSql().contains("voided_"))
            .findFirst()
            .orElse(null);

        if (renameChange == null) {
            fail("No rename change found for voided column. Changes found: " + 
                  changes.stream().map(c -> c.getClass().getSimpleName()).toList());
        }

        // Extract the voided name from the SQL
        String sql = renameChange.getSql();
        int toIndex = sql.indexOf(" to \"");
        int endIndex = sql.lastIndexOf("\"");
        String voidedName = sql.substring(toIndex + 5, endIndex);
        
        System.out.println("Original column name: " + columnMetadata.getName());
        System.out.println("Generated voided name: " + voidedName);
        System.out.println("Voided name length: " + voidedName.length());

        // CRITICAL: The voided name MUST NOT exceed 63 characters
        if (voidedName.length() > 63) {
            fail("REGRESSION DETECTED: Voided column name exceeds PostgreSQL limit of 63 characters: " + 
                  voidedName + " (length: " + voidedName.length() + ")");
        }

        // Must start with voided_
        assertTrue(voidedName.startsWith("voided_"), 
            "Voided name must start with 'voided_': " + voidedName);

        // If the original column was truncated, the voided column should also be properly handled
        if (columnMetadata.getName().matches(".*\\s\\(\\d+\\)$")) {
            // For long names that need truncation, should have hashcode
            if (voidedName.length() > 57) {
                assertTrue(voidedName.matches(".*\\s\\(\\d+\\)$"), 
                    "Long voided names must have hashcode: " + voidedName);
            }
        }

        // Must not be the problematic name from the original bug
        assertNotEquals("voided_Please provide name of child and information about suffe", voidedName,
            "Should not produce the problematic name from the original bug");
    }

    @Test
    void ensureTableMetadataUsesCorrectMethodForVoidedColumns() {
        // This test specifically checks that TableMetadata.findChanges() uses the correct method
        // and not the buggy getVoidedName() directly
        
        // Set up context
        OrgIdentityContextHolder.setContext(
            OrganisationIdentity.createForOrganisation("test_user", "test_schema", "test")
        );

        // Create the scenario where findChanges() would be called
        TableMetadata existingTable = new TableMetadata();
        existingTable.setName("test_table");
        
        // Use a name that when voided would exceed 63 chars without proper truncation
        String problematicName = "Please provide name of child and information about suffering from disease";
        Column column = new Column(problematicName, Column.Type.text);
        ColumnMetadata columnMetadata = new ColumnMetadata(
            column, 1, ColumnMetadata.ConceptType.Text, "concept-uuid", false
        );
        existingTable.addColumnMetadata(List.of(columnMetadata));

        // New table without the column (simulating form element removal)
        TableMetadata newTable = new TableMetadata();
        newTable.setName("test_table");

        // What the buggy method would produce
        String buggyResult = columnMetadata.getVoidedName();
        
        // What the correct method produces
        String correctResult = columnMetadata.getNewVoidedColumnMetaData().getName();
        
        System.out.println("Buggy method result: " + buggyResult + " (length: " + buggyResult.length() + ")");
        System.out.println("Correct method result: " + correctResult + " (length: " + correctResult.length() + ")");

        // Call the actual findChanges() method
        List<org.avniproject.etl.domain.metadata.diff.Diff> changes = newTable.findChanges(existingTable);
        
        // Extract the voided name from the actual implementation
        String actualResult = null;
        for (org.avniproject.etl.domain.metadata.diff.Diff diff : changes) {
            if (diff instanceof org.avniproject.etl.domain.metadata.diff.RenameColumn) {
                org.avniproject.etl.domain.metadata.diff.RenameColumn rc = 
                    (org.avniproject.etl.domain.metadata.diff.RenameColumn) diff;
                String sql = rc.getSql();
                if (sql.contains("voided_")) {
                    int toIndex = sql.indexOf(" to \"");
                    int endIndex = sql.lastIndexOf("\"");
                    actualResult = sql.substring(toIndex + 5, endIndex);
                    break;
                }
            }
        }
        
        assertNotNull(actualResult, "Should have a rename change with voided column");
        System.out.println("Actual implementation result: " + actualResult + " (length: " + actualResult.length() + ")");

        // If the buggy method exceeds the limit, the actual implementation must use the correct method
        if (buggyResult.length() > 63) {
            // The actual implementation must not use the buggy method
            assertNotEquals(buggyResult, actualResult, 
                "Implementation must not use the buggy getVoidedName() method directly");
            assertEquals(correctResult, actualResult,
                "Implementation must use the correct getNewVoidedColumnMetaData() method");
            assertTrue(actualResult.length() <= 63, 
                "Actual implementation must produce name within limit");
        }
    }
}
