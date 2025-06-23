package org.avniproject.etl.repository.sync;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.service.RepeatableQuestionGroupMediaColumnProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

import static org.avniproject.etl.domain.metadata.ColumnMetadata.MEDIA_COLUMN_CONCEPT_TYPES;

@Repository
public class MediaForRepeatableGroupsSyncAction implements EntitySyncAction {
    private static final Logger logger = Logger.getLogger(MediaForRepeatableGroupsSyncAction.class);
    private final RepeatableQuestionGroupMediaColumnProcessingService repeatableQuestionGroupMediaColumnProcessingService;

    @Autowired
    public MediaForRepeatableGroupsSyncAction(RepeatableQuestionGroupMediaColumnProcessingService repeatableQuestionGroupMediaColumnProcessingService) {
        this.repeatableQuestionGroupMediaColumnProcessingService = repeatableQuestionGroupMediaColumnProcessingService;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !tableMetadata.getType().equals(TableMetadata.Type.Media);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        // Only process if this is a media table sync job
        if (doesntSupport(tableMetadata)) {
            logger.info("Skipping sync for non-media table: " + tableMetadata.getName());
            return;
        }

        logger.info("Starting sync from repeatable question groups for media table: " + tableMetadata.getName());
        currentSchemaMetadata.getTableMetadata().forEach(table -> {
            boolean isRQG = table.isRepeatableQuestionGroupTable();
            if (isRQG) {
                logger.info("Processing repeatable question group table: " + table.getName());
                processRepeatableQuestionGroupTable(tableMetadata, table, lastSyncTime, dataSyncBoundaryTime);
            }
        });
    }

    private void processRepeatableQuestionGroupTable(TableMetadata mediaTableMetadata, TableMetadata tableData, Date lastSyncTime, Date dataSyncBoundaryTime) {
        List<ColumnMetadata> conceptMediaColumns = tableData.findColumnsMatchingConceptType(MEDIA_COLUMN_CONCEPT_TYPES);

        logger.info("Found " + conceptMediaColumns.size() + " media columns in table: " + tableData.getName());

        if (conceptMediaColumns.isEmpty()) {
            return; // No media columns found
        }

        for (ColumnMetadata mediaColumn : conceptMediaColumns) {
            try {
                logger.info("Syncing media from column: " + mediaColumn.getName() +
                        ", ConceptUUID: " + mediaColumn.getConceptUuid());

                repeatableQuestionGroupMediaColumnProcessingService.processMediaColumn(
                        mediaTableMetadata,
                        tableData,
                        mediaColumn,
                        lastSyncTime,
                        dataSyncBoundaryTime);
            } catch (Exception e) {
                logger.error("Transaction error for column " + mediaColumn.getName() + ": " + e.getMessage(), e);
                // Continue with the next column
            }
        }
    }
}
