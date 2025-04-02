WITH last_sam_mam AS (SELECT ind.id  AS "Individual ID",
                             CASE
                                 WHEN ROW_NUMBER() OVER (
                                     PARTITION BY follow_up.individual_id, follow_up.program_enrolment_id
                                     ORDER BY follow_up.encounter_date_time DESC NULLS LAST
                                     ) = 1
                                     AND follow_up."Weight for Height Status" = 'SAM'
                                     AND follow_up.last_modified_date_time > :previousCutoffDateTime 
                                     AND follow_up.last_modified_date_time <= :newCutoffDateTime
                                     THEN follow_up.encounter_date_time
                                 ELSE NULL
                                 END AS "Date of Last SAM",
                             CASE
                                 WHEN ROW_NUMBER() OVER (
                                     PARTITION BY follow_up.individual_id, follow_up.program_enrolment_id
                                     ORDER BY follow_up.encounter_date_time DESC NULLS LAST
                                     ) = 1
                                     AND follow_up."Weight for Height Status" = 'MAM'
                                     AND follow_up.last_modified_date_time > :previousCutoffDateTime 
                                     AND follow_up.last_modified_date_time <= :newCutoffDateTime
                                     THEN follow_up.encounter_date_time
                                 ELSE NULL
                                 END AS "Date of Last MAM"
                      FROM apfodisha.individual ind
                               LEFT JOIN
                           apfodisha.individual_child enrl ON
                                       enrl.individual_id = ind.id
                                   AND enrl.is_voided = false
                               LEFT JOIN
                           apfodisha.individual_child_growth_monitoring follow_up ON
                                       follow_up.program_enrolment_id = enrl.id
                                   AND follow_up.encounter_date_time IS NOT NULL
                                   AND follow_up.last_modified_date_time > :previousCutoffDateTime 
                                   AND follow_up.last_modified_date_time <= :newCutoffDateTime
                                   AND follow_up.is_voided = false)
UPDATE
    apfodisha.individual_child_growth_monitoring_report growth_report
SET "Date of Last SAM" = lsm."Date of Last SAM",
    "Date of Last MAM" = lsm."Date of Last MAM"
FROM last_sam_mam lsm
WHERE growth_report."Beneficiary ID" = lsm."Individual ID";
