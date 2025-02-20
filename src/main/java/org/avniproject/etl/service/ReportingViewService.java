package org.avniproject.etl.service;

import org.avniproject.etl.domain.Organisation;
import org.avniproject.etl.domain.OrganisationIdentity;
import org.avniproject.etl.repository.ReportingViewRepository;
import org.springframework.stereotype.Service;

@Service
public class ReportingViewService {
    private final ReportingViewRepository reportingViewRepository;
    public ReportingViewService(ReportingViewRepository reportingViewRepository) {
        this.reportingViewRepository = reportingViewRepository;
    }

    public void processMetabaseViews(Organisation organisation) {
        OrganisationIdentity organisationIdentity = organisation.getOrganisationIdentity();
        reportingViewRepository.createOrReplaceView(organisationIdentity);
    }
}
