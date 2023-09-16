package org.avniproject.etl.controller;

import org.avniproject.etl.dto.AggregateReportResult;
import org.avniproject.etl.dto.UserActivityDTO;
import org.avniproject.etl.repository.ReportRepository;
import org.avniproject.etl.util.ReportUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class ReportController {

    private final ReportRepository reportRepository;
    private final ReportUtil reportUtil;

    @Autowired
    public ReportController(ReportRepository reportRepository, ReportUtil reportUtil) {
        this.reportRepository = reportRepository;
        this.reportUtil = reportUtil;
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/aggregate/summaryTable", method = RequestMethod.GET)
    public List<UserActivityDTO> getSummaryTable(){
        return reportRepository.generateSummaryTable();
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "report/hr/userActivity", method = RequestMethod.GET)
    public List<UserActivityDTO> getUserActivity(@RequestParam(value = "startDate", required = false) String startDate,
                                          @RequestParam(value = "endDate", required = false) String endDate,
                                          @RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds){
            return reportRepository.generateUserActivity(
                   reportUtil.getDateDynamicWhere(startDate, endDate, "registration_date"),
                   reportUtil.getDateDynamicWhere(startDate, endDate, "encounter_date_time"),
                   reportUtil.getDateDynamicWhere(startDate, endDate, "enrolment_date_time"),
                   reportUtil.getDynamicUserWhere(userIds, "u.id")
            );
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/syncFailures",method = RequestMethod.GET)
    public List<UserActivityDTO> getUserWiseSyncFailures(@RequestParam(value = "startDate", required = false) String startDate,
                                                  @RequestParam(value = "endDate", required = false) String endDate,
                                                  @RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds){
            return reportRepository.generateUserSyncFailures(
                    reportUtil.getDateDynamicWhere(startDate, endDate, "st.sync_start_time"),
                    reportUtil.getDynamicUserWhere(userIds, "u.id")
            );
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/deviceModels", method = RequestMethod.GET)
    public List<AggregateReportResult> getUserWiseDeviceModels(@RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds) {

        return reportRepository.generateUserDeviceModels(
                reportUtil.getDynamicUserWhere(userIds, "u.id"));
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/appVersions", method = RequestMethod.GET)
    public List<AggregateReportResult> getUserWiseAppVersions(@RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds) {

        return reportRepository.generateUserAppVersions(
                reportUtil.getDynamicUserWhere(userIds, "u.id"));
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/userDetails", method = RequestMethod.GET)
    public List<UserActivityDTO> getUserDetails(@RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds) {

        return reportRepository.generateUserDetails(
                reportUtil.getDynamicUserWhere(userIds, "u.id"));
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/latestSyncs", method = RequestMethod.GET)
    public List<UserActivityDTO> getLatestSyncs(@RequestParam(value = "startDate", required = false) String startDate,
                                                @RequestParam(value = "endDate", required = false) String endDate,
                                                @RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds) {

        return reportRepository.generateLatestSyncs(
                reportUtil.getDateDynamicWhere(startDate, endDate, "st.sync_end_time"),
                reportUtil.getDynamicUserWhere(userIds, "u.id"));
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/medianSync", method = RequestMethod.GET)
    public List<UserActivityDTO> getMedianSync(@RequestParam(value = "startDate", required = false) String startDate,
                                                @RequestParam(value = "endDate", required = false) String endDate,
                                                @RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds) {

        return reportRepository.generateMedianSync(
                reportUtil.getDateSeries(startDate, endDate));
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/championUsers", method = RequestMethod.GET)
    public List<AggregateReportResult> getChampionUsers(@RequestParam(value = "startDate", required = false) String startDate,
                                                        @RequestParam(value = "endDate", required = false) String endDate,
                                                        @RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds) {
        return reportRepository.generateCompletedVisitsOnTimeByProportion(
                ">= 0.5",
                reportUtil.getDateDynamicWhere(startDate, endDate, "encounter_date_time"),
                reportUtil.getDynamicUserWhere(userIds, "u.id"));
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/nonPerformingUsers", method = RequestMethod.GET)
    public List<AggregateReportResult> getNonPerformingUsers(@RequestParam(value = "startDate", required = false) String startDate,
                                                             @RequestParam(value = "endDate", required = false) String endDate,
                                                             @RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds) {
        return reportRepository.generateCompletedVisitsOnTimeByProportion(
                "<= 0.5",
                reportUtil.getDateDynamicWhere(startDate, endDate, "encounter_date_time"),
                reportUtil.getDynamicUserWhere(userIds, "u.id")
        );
    }

    @PreAuthorize("hasAnyAuthority('analytics_user')")
    @RequestMapping(value = "/report/hr/mostCancelled", method = RequestMethod.GET)
    public List<AggregateReportResult> getUsersCancellingMostVisits(@RequestParam(value = "startDate", required = false) String startDate,
                                                                    @RequestParam(value = "endDate", required = false) String endDate,
                                                                    @RequestParam(value = "userIds", required = false, defaultValue = "") List<Long> userIds) {
        return reportRepository.generateUserCancellingMostVisits(
                reportUtil.getDateDynamicWhere(startDate, endDate, "encounter_date_time"),
                reportUtil.getDynamicUserWhere(userIds, "u.id"));
    }


}

