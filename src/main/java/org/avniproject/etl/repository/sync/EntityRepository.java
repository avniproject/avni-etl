package org.avniproject.etl.repository.sync;

import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Repository
public class EntityRepository {
    private final List<EntitySyncAction> entitySyncRepositories = new ArrayList<>();

    @Autowired
    public EntityRepository(TransactionalTablesSyncAction transactionalTablesSyncAction,
                            DuplicateRowDeleteAction duplicateRowDeleteAction,
                            AddressTableSyncAction addressTableSyncAction,
                            EntityApprovalStatusSyncAction entityApprovalStatusSyncAction,
                            AnswerConceptSync answerConceptSync,
                            MediaTableSyncAction mediaTableSyncAction,
                            SyncTelemetryTableSyncAction syncTelemetryTableSyncAction,
                            UserTableSyncAction userTableSyncAction,
                            CleanEnrolmentExitTableAction cleanEnrolmentExitTableAction,
                            CleanEncounterCancelTableAction cleanEncounterCancelTableAction,
                            RepeatableQGTransactionTablesSyncAction repeatableQGTransactionTablesSyncAction) {
        entitySyncRepositories.add(transactionalTablesSyncAction);
        entitySyncRepositories.add(addressTableSyncAction);
        entitySyncRepositories.add(repeatableQGTransactionTablesSyncAction);
        entitySyncRepositories.add(entityApprovalStatusSyncAction);
        entitySyncRepositories.add(answerConceptSync);
        entitySyncRepositories.add(mediaTableSyncAction);
        entitySyncRepositories.add(syncTelemetryTableSyncAction);
        entitySyncRepositories.add(userTableSyncAction);
        entitySyncRepositories.add(duplicateRowDeleteAction);
        entitySyncRepositories.add(cleanEncounterCancelTableAction);
        entitySyncRepositories.add(cleanEnrolmentExitTableAction);
    }

    @Transactional
    public void saveEntities(TableMetadata tableMetadata, Date lastSyncTime, Date dataSyncBoundaryTime, SchemaMetadata currentSchemaMetadata) {
        entitySyncRepositories.forEach(entitySyncRepository -> {
            entitySyncRepository.perform(tableMetadata, lastSyncTime, dataSyncBoundaryTime, currentSchemaMetadata);
        });
    }
}
