package org.avniproject.etl.service;

import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.repository.ReportingViewRepository;
import org.avniproject.etl.repository.sync.MultiSelectViewSyncAction;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

@Service
public class ReportingViewService {
    private final ReportingViewRepository reportingViewRepository;
    private final MultiSelectViewSyncAction multiSelectViewSyncAction;
    private static final Logger log = Logger.getLogger(ReportingViewService.class);

    public ReportingViewService(ReportingViewRepository reportingViewRepository,
                              MultiSelectViewSyncAction multiSelectViewSyncAction) {
        this.reportingViewRepository = reportingViewRepository;
        this.multiSelectViewSyncAction = multiSelectViewSyncAction;
    }

    public void processViews(Organisation organisation) {
        OrganisationIdentity organisationIdentity = organisation.getOrganisationIdentity();
        reportingViewRepository.createOrReplaceView(organisationIdentity);
        log.info(String.format("Created reporting views for %s", organisationIdentity));
        multiSelectViewSyncAction.createMultiselectViews(organisationIdentity, organisation.getSchemaMetadata());
        log.info(String.format("Created multiselect views for %s", organisationIdentity));
    }
}
