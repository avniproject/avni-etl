package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.metadata.ColumnMetadata;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.service.MediaColumnProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public class MediaForRepeatableGroupsSyncAction implements EntitySyncAction {
    private final MediaColumnProcessingService mediaColumnProcessingService;

    @Autowired
    public MediaForRepeatableGroupsSyncAction(MediaColumnProcessingService mediaColumnProcessingService) {
        this.mediaColumnProcessingService = mediaColumnProcessingService;
    }

    @Override
    public boolean doesntSupport(TableMetadata tableMetadata) {
        return !tableMetadata.getType().equals(TableMetadata.Type.Media);
    }

    @Override
    public void perform(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        // Only process if this is a media table sync job
        if (tableMetadata.getType() != TableMetadata.Type.Media) {
            System.out.println("[MEDIA_RQG_SYNC] Skipping sync for non-media table: " + tableMetadata.getName());
            return;
        }

        System.out.println("[MEDIA_RQG_SYNC] Starting sync from repeatable question groups for media table: " + tableMetadata.getName());
        currentSchemaMetadata.getTableMetadata().forEach(table -> {
            boolean isRQG = isRepeatableQuestionGroupTable(table);
            if (isRQG) {
                System.out.println("[MEDIA_RQG_SYNC] Processing repeatable question group table: " + table.getName());
                processRepeatableQuestionGroupTable(tableMetadata, table, lastSyncTime, dataSyncBoundaryTime);
            }
        });
    }

    private boolean isRepeatableQuestionGroupTable(TableMetadata tableMetadata) {
        boolean isRqgType = tableMetadata.getType() == TableMetadata.Type.RepeatableQuestionGroup;
        boolean hasConceptUuid = tableMetadata.getRepeatableQuestionGroupConceptUuid() != null;
        return isRqgType && hasConceptUuid;
    }

    private void processRepeatableQuestionGroupTable(TableMetadata mediaTableMetadata, TableMetadata tableData, Date lastSyncTime, Date dataSyncBoundaryTime) {
        List<ColumnMetadata> conceptMediaColumns = tableData.findColumnsMatchingConceptType(
                ColumnMetadata.ConceptType.Image,
                ColumnMetadata.ConceptType.ImageV2,
                ColumnMetadata.ConceptType.Video,
                ColumnMetadata.ConceptType.Audio,
                ColumnMetadata.ConceptType.File
        );

        System.out.println("[MEDIA_RQG_SYNC] Found " + conceptMediaColumns.size() + " media columns in table: " + tableData.getName());

        if (conceptMediaColumns.isEmpty()) {
            return; // No media columns found
        }

        for (ColumnMetadata mediaColumn : conceptMediaColumns) {
            try {
                System.out.println("[MEDIA_RQG_SYNC] Syncing media from column: " + mediaColumn.getName() +
                        ", ConceptUUID: " + mediaColumn.getConceptUuid());

                mediaColumnProcessingService.processMediaColumn(
                        mediaTableMetadata,
                        tableData,
                        mediaColumn,
                        lastSyncTime,
                        dataSyncBoundaryTime);
            } catch (Exception e) {
                System.out.println("[MEDIA_RQG_SYNC] Transaction error for column " + mediaColumn.getName() + ": " + e.getMessage());
                e.printStackTrace(); // Print stack trace for debugging
                // Continue with the next column
            }
        }
    }
}
