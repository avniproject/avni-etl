WITH cte_nrc_status AS (SELECT DISTINCT ind.id  AS "Individual ID",
                                        CASE
                                            WHEN nrc."Admission Status" = 'Admited' THEN 'Yes'
                                            WHEN nrc."Admission Status" IS NULL THEN 'No'
                                            ELSE 'No'
                                            END AS "Was the child admitted to NRC before"
                        FROM apfodisha.individual ind
                                 JOIN apfodisha.individual_child enrl
                                      ON enrl.individual_id = ind.id
                                 LEFT JOIN apfodisha.individual_child_growth_monitoring follow_up
                                           ON follow_up.program_enrolment_id = enrl.id
                                               AND follow_up.encounter_date_time IS NOT NULL
                                 LEFT JOIN apfodisha.individual_child_qrt_child qrt
                                           ON ind.id = follow_up.individual_id
                                               AND qrt.encounter_date_time IS NOT NULL
                                 LEFT JOIN apfodisha.individual_child_nrc_admission nrc
                                           ON nrc.individual_id = qrt.individual_id
                                               AND qrt.encounter_date_time IS NOT null
                        WHERE (qrt.last_modified_date_time > :previousCutoffDateTime AND qrt.last_modified_date_time <= :newCutoffDateTime)
                           OR (nrc.last_modified_date_time > :previousCutoffDateTime AND nrc.last_modified_date_time <= :newCutoffDateTime))
UPDATE apfodisha.individual_child_growth_monitoring_report growth_report
SET "Was the child admitted to NRC before" = cte."Was the child admitted to NRC before"
FROM cte_nrc_status cte
WHERE growth_report."Beneficiary ID" = cte."Individual ID";
