package org.avniproject.etl.service;

import org.apache.log4j.Logger;
import org.avniproject.etl.domain.OrgIdentityContextHolder;
import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.metadata.SchemaMetadata;
import org.avniproject.etl.domain.metadata.TableMetadata;
import org.avniproject.etl.domain.syncstatus.EntitySyncStatus;
import org.avniproject.etl.domain.syncstatus.SchemaDataSyncStatus;
import org.avniproject.etl.repository.EntitySyncStatusRepository;
import org.avniproject.etl.repository.sync.EntityRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class SyncService {

    private final EntityRepository entityRepository;
    private final EntitySyncStatusRepository entitySyncStatusRepository;
    private static final Logger log = Logger.getLogger(SyncService.class);

    @Autowired
    public SyncService(EntityRepository entityRepository, EntitySyncStatusRepository entitySyncStatusRepository) {
        this.entityRepository = entityRepository;
        this.entitySyncStatusRepository = entitySyncStatusRepository;
    }

    /**
     * In case of an error, we do not want the read only db to be in an inconsistent state wrt
     * related data because it can cause unexpected scenarios in reports. We roll back on an
     * organisation basis.
     * @param organisation
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void sync(Organisation organisation) {
        SchemaMetadata currentSchemaMetadata = organisation.getSchemaMetadata();
        currentSchemaMetadata.getOrderedTableMetadata().stream().filter(TableMetadata::isPartOfRegularSync)
                .forEach(tableMetadata -> migrateTable(tableMetadata, organisation.getSyncStatus(), currentSchemaMetadata));
    }

    @Transactional
    public void migrateTable(TableMetadata tableMetadata, SchemaDataSyncStatus syncStatus, SchemaMetadata currentSchemaMetadata) {
        log.info(String.format("Migrating table %s.%s of type %s", OrgIdentityContextHolder.getDbSchema(), tableMetadata.getName(), tableMetadata.getType()));
        EntitySyncStatus entitySyncStatus = syncStatus.startSync(tableMetadata, OrgIdentityContextHolder.getOrganisationIdentity());
        entitySyncStatusRepository.save(entitySyncStatus);

        entityRepository.saveEntities(tableMetadata, entitySyncStatus.getLastSyncTime(), OrgIdentityContextHolder.dataSyncBoundaryTime(), currentSchemaMetadata);

        entitySyncStatus.markSuccess(OrgIdentityContextHolder.dataSyncBoundaryTime());
        entitySyncStatusRepository.save(entitySyncStatus);
    }
}
