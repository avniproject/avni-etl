WITH filtered_growth_data AS (
    SELECT
        individual_id,
        "Weight for Height Status",
        encounter_date_time
    FROM apfodisha.individual_child_growth_monitoring follow_up
    WHERE
        is_voided = false
        AND encounter_date_time >= (:newCutoffDateTime::date - INTERVAL '3 months')
        AND encounter_date_time < :newCutoffDateTime
        AND follow_up.last_modified_date_time > :previousCutoffDateTime
        AND follow_up.last_modified_date_time <= :newCutoffDateTime
),
consistency_status AS (
    SELECT
        individual_id AS "Individual ID",
        -- Strict SAM check
        COALESCE(BOOL_AND("Weight for Height Status" = 'SAM')
                 FILTER (WHERE encounter_date_time >= (:newCutoffDateTime::date - INTERVAL '1 month')), FALSE) AS consistently_1_month_severe,
        COALESCE(BOOL_AND("Weight for Height Status" = 'SAM')
                 FILTER (WHERE encounter_date_time >= (:newCutoffDateTime::date - INTERVAL '2 months')), FALSE) AS consistently_2_month_severe,
        COALESCE(BOOL_AND("Weight for Height Status" = 'SAM')
                 FILTER (WHERE encounter_date_time >= (:newCutoffDateTime::date - INTERVAL '3 months')), FALSE) AS consistently_3_month_severe,
        -- Strict MAM check
        COALESCE(BOOL_AND("Weight for Height Status" = 'MAM')
                 FILTER (WHERE encounter_date_time >= (:newCutoffDateTime::date - INTERVAL '1 month')), FALSE) AS consistently_1_month_moderate,
        COALESCE(BOOL_AND("Weight for Height Status" = 'MAM')
                 FILTER (WHERE encounter_date_time >= (:newCutoffDateTime::date - INTERVAL '2 months')), FALSE) AS consistently_2_month_moderate,
        COALESCE(BOOL_AND("Weight for Height Status" = 'MAM')
                 FILTER (WHERE encounter_date_time >= (:newCutoffDateTime::date - INTERVAL '3 months')), FALSE) AS consistently_3_month_moderate
    FROM filtered_growth_data
    GROUP BY individual_id
)
UPDATE apfodisha.individual_child_growth_monitoring_report growth_report
SET
    "Was the Child SAM in the Last 1 month" = CASE WHEN cte.consistently_1_month_severe THEN 'Yes' ELSE 'No' END,
    "Was the Child SAM in the Last 2 month" = CASE WHEN cte.consistently_2_month_severe THEN 'Yes' ELSE 'No' END,
    "Was the Child SAM in the Last 3 Months" = CASE WHEN cte.consistently_3_month_severe THEN 'Yes' ELSE 'No' END,
    "Was the Child MAM in the Last 1 Month" = CASE WHEN cte.consistently_1_month_moderate THEN 'Yes' ELSE 'No' END,
    "Was the Child MAM in the Last 2 Months" = CASE WHEN cte.consistently_2_month_moderate THEN 'Yes' ELSE 'No' END,
    "Was the Child MAM in the Last 3 Months" = CASE WHEN cte.consistently_3_month_moderate THEN 'Yes' ELSE 'No' END
FROM consistency_status cte
WHERE growth_report."Beneficiary ID" = cte."Individual ID";