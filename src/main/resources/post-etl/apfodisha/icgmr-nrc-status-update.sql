WITH cte_nrc_status AS (
    SELECT individual_id AS "Individual ID",
    MAX(CASE WHEN "Admission Status" = 'Admited' THEN 'Yes' ELSE 'No' END) AS "Was the child admitted to NRC before"
    FROM apfodisha.individual_child_nrc_admission nrc
    WHERE is_voided = FALSE
    AND (nrc.last_modified_date_time > :previousCutoffDateTime AND nrc.last_modified_date_time <= :newCutoffDateTime)
    GROUP BY individual_id
)
UPDATE apfodisha.individual_child_growth_monitoring_report growth_report
SET "Was the child admitted to NRC before" = cte."Was the child admitted to NRC before"
FROM cte_nrc_status cte
WHERE growth_report."Beneficiary ID" = cte."Individual ID";