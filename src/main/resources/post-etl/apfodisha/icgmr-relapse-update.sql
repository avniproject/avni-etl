WITH relapse_data AS (SELECT DISTINCT ind.id  AS "Individual ID",
                                      CASE
                                          WHEN EXISTS (SELECT 1
                                                       FROM apfodisha.individual_child_growth_monitoring f
                                                       WHERE f.individual_id = follow_up.individual_id
                                                         AND f.program_enrolment_id = follow_up.program_enrolment_id
                                                         AND f."Weight for Height Status" = 'SAM'
                                                         AND f.encounter_date_time < date_trunc('month', CURRENT_DATE) - INTERVAL '1 month' * :month_interval
                                                         AND f.last_modified_date_time > :previousCutoffDateTime 
                                                         AND f.last_modified_date_time <= :newCutoffDateTime)
                                              AND follow_up."Weight for Height Status" = 'SAM'
                                              AND NOT EXISTS (SELECT 1
                                                               FROM apfodisha.individual_child_growth_monitoring f2
                                                               WHERE f2.individual_id = follow_up.individual_id
                                                                AND f2.program_enrolment_id = follow_up.program_enrolment_id
                                                                AND f2."Weight for Height Status" = 'SAM'
                                                                AND f2.encounter_date_time >= date_trunc('month', CURRENT_DATE) - INTERVAL '1 month' * :month_interval
                                                                AND f2.last_modified_date_time > :previousCutoffDateTime 
                                                                AND f2.last_modified_date_time <= :newCutoffDateTime)
                                              THEN 'Yes'
                                          ELSE 'No'
                                          END AS "Is it a relapse child"
                      FROM apfodisha.individual ind
                               JOIN apfodisha.individual_child enrl ON
                                  enrl.individual_id = ind.id
                               LEFT JOIN apfodisha.individual_child_growth_monitoring follow_up ON
                                  follow_up.program_enrolment_id = enrl.id
                              AND follow_up.encounter_date_time IS NOT NULL
                              AND follow_up.is_voided = false
                              AND follow_up.last_modified_date_time > :previousCutoffDateTime 
                              AND follow_up.last_modified_date_time <= :newCutoffDateTime)
UPDATE apfodisha.individual_child_growth_monitoring_report growth_report
SET "Is it a relapse child" = cte."Is it a relapse child"
FROM relapse_data cte
WHERE growth_report."Beneficiary ID" = cte."Individual ID";
