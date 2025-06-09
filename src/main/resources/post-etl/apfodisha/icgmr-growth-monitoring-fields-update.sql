WITH growth_monitoring_fields AS (
    SELECT
        CASE
            WHEN follow_up."Weight for age Status" = 'Severely Underweight' THEN 'Yes'
            ELSE 'No'
        END AS "Severely Underweight",
        CASE
            WHEN follow_up."Weight for age Status" = 'Moderately Underweight' THEN 'Yes'
            ELSE 'No'
        END AS "Moderately Underweight",
        CASE
            WHEN follow_up."Weight for Height Status" = 'SAM' THEN 'Yes'
            ELSE 'No'
        END AS "SAM",
        CASE
            WHEN follow_up."Weight for Height Status" = 'MAM' THEN 'Yes'
            ELSE 'No'
        END AS "MAM",
        CASE
            WHEN follow_up."Height for age Status" = 'Stunted' THEN 'Yes'
            ELSE 'No'
        END AS "Moderately Stunted",
        CASE
            WHEN follow_up."Height for age Status" = 'Severely stunted' THEN 'Yes'
            ELSE 'No'
        END AS "Severely Stunted",
        follow_up."Growth Faltering" AS "Growth Falter Status (GF-1/GF-2)",
        follow_up."Height",
        follow_up."Weight",
        follow_up."Nutritional Status",
        follow_up."encounter_date_time" AS "Date of Measurement",
        ind.id AS "Beneficiary ID",
        follow_up."Is the child going to PPK?",
        follow_up."Is the child going to Creche?",
        follow_up."Is the child being currently exclusively breastfed?",
        follow_up."Is the child being currently breastfed?",
        follow_up."Number of day attended AWC (last month)",
        follow_up."Is the child receiving egg from AWC",
        follow_up."To be monitored by QRT",
        follow_up."Is the child receiving THR from AWC",
        follow_up."Did the child attended VHND last month",
        follow_up."What is the treatment advise for the SAM/MAM/GF2 child?",
        follow_up."Is the child enrolled in the CMAM program?",
        follow_up."Is the child availing benefits (ATHR) under the CMAM program?",
        follow_up."Did you receive additional THR (MSPY)?",
        CASE
            WHEN enrl.program_exit_date_time IS NULL THEN 'No'
            ELSE 'Yes'
        END AS "Program Exited"
    FROM apfodisha.individual ind
    JOIN apfodisha.individual_child enrl
        ON enrl.individual_id = ind.id
        AND enrl.enrolment_date_time IS NOT NULL
        AND ind.is_voided = false
    LEFT JOIN apfodisha.individual_child_growth_monitoring follow_up
        ON follow_up.program_enrolment_id = enrl.id
        AND follow_up.encounter_date_time IS NOT NULL
        AND follow_up.is_voided = false
        AND follow_up.created_date_time < :previousCutoffDateTime
        AND follow_up.last_modified_date_time > :previousCutoffDateTime
        AND follow_up.last_modified_date_time <= :newCutoffDateTime
)
UPDATE apfodisha.individual_child_growth_monitoring_report growth_report
SET "Severely Underweight"                                          = fld."Severely Underweight",
    "Moderately Underweight"                                        = fld."Moderately Underweight",
    "SAM"                                                           = fld."SAM",
    "MAM"                                                           = fld."MAM",
    "Moderately Stunted"                                            = fld."Moderately Stunted",
    "Severely Stunted"                                              = fld."Severely Stunted",
    "Growth Falter Status (GF-1/GF-2)"                              = fld."Growth Falter Status (GF-1/GF-2)",
    "Height"                                                        = fld."Height",
    "Weight"                                                        = fld."Weight",
    "Nutritional Status"                                            = fld."Nutritional Status",
    "Date of Measurement"                                           = fld."Date of Measurement",
    "Is the child going to PPK?"                                    = fld."Is the child going to PPK?",
    "Is the child going to Creche?"                                 = fld."Is the child going to Creche?",
    "Is the child being currently exclusively breastfed?"           = fld."Is the child being currently exclusively breastfed?",
    "Is the child being currently breastfed?"                       = fld."Is the child being currently breastfed?",
    "Number of day attended AWC (last month)"                       = fld."Number of day attended AWC (last month)",
    "Is the child receiving egg from AWC"                           = fld."Is the child receiving egg from AWC",
    "To be monitored by QRT"                                        = fld."To be monitored by QRT",
    "Is the child receiving THR from AWC"                           = fld."Is the child receiving THR from AWC",
    "Did the child attended VHND last month"                        = fld."Did the child attended VHND last month",
    "What is the treatment advise for the SAM/MAM/GF2 child?"       = fld."What is the treatment advise for the SAM/MAM/GF2 child?",
    "Is the child enrolled in the CMAM program?"                    = fld."Is the child enrolled in the CMAM program?",
    "Is the child availing benefits (ATHR) under the CMAM program?" = fld."Is the child availing benefits (ATHR) under the CMAM program?",
    "Did you receive additional THR (MSPY)?"                        = fld."Did you receive additional THR (MSPY)?",
    "Program Exited"                                                = fld."Program Exited"
FROM growth_monitoring_fields fld
WHERE growth_report."Beneficiary ID" = fld."Beneficiary ID"
AND growth_report."Date of Measurement" = fld."Date of Measurement";