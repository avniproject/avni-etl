WITH growth_monitoring_fields AS (SELECT CASE
                                             WHEN follow_up."Weight for age Status" = 'Severely Underweight' THEN 'Yes'
                                             ELSE 'No'
                                             END                       AS "Severely Underweight",
                                         CASE
                                             WHEN follow_up."Weight for age Status" = 'Moderately Underweight' THEN 'Yes'
                                             ELSE 'No'
                                             END                       AS "Moderately Underweight",
                                         CASE
                                             WHEN follow_up."Weight for Height Status" = 'SAM' THEN 'Yes'
                                             ELSE 'No'
                                             END                       AS "SAM",
                                         CASE
                                             WHEN follow_up."Weight for Height Status" = 'MAM' THEN 'Yes'
                                             ELSE 'No'
                                             END                       AS "MAM",
                                         CASE
                                             WHEN follow_up."Height for age Status" = 'Moderately Stunted' THEN 'Yes'
                                             ELSE 'No'
                                             END                       AS "Moderately Stunted",
                                         CASE
                                             WHEN follow_up."Height for age Status" = 'Severely Stunted' THEN 'Yes'
                                             ELSE 'No'
                                             END                       AS "Severely Stunted",
                                         follow_up."Growth Faltering"  AS "Growth Falter Status (GF-1/GF-2)",
                                         follow_up."Height"            AS "Height",
                                         follow_up."Weight"            AS "Weight",
                                         follow_up."Date of recording" AS "Date of Measurement",
                                         ind.id                        AS "Individual ID",
                                         follow_up."PPK"               AS "Is the child going to PPK?",
                                         follow_up."Creche"            AS "Is the child going to Creche?",
                                         follow_up."Exclusive breastfeeding" AS "Is the child being currently exclusively breastfed?",
                                         follow_up."Breast feeding"    AS "Is the child being currently breastfed?",
                                         follow_up."Number of days attended AWC" AS "Number of day attended AWC (last month)",
                                         follow_up."Egg"               AS "Is the child receiving egg from AWC",
                                         follow_up."THR"               AS "Is the child receiving THR from AWC",
                                         follow_up."VHND"             AS "Did the child attended VHND last month",
                                         follow_up."Treatment advice"  AS "What is the treatment advise for the SAM/MAM/GF2 child?",
                                         follow_up."CMAM"             AS "Is the child enrolled in the CMAM program?",
                                         follow_up."ATHR"             AS "Is the child availing benefits (ATHR) under the CMAM program?",
                                         follow_up."MSPY"             AS "Did you receive additional THR (MSPY)?"
                                  FROM apfodisha.individual ind
                                           JOIN apfodisha.individual_child enrl ON
                                              enrl.individual_id = ind.id
                                          AND enrl.enrolment_date_time IS NOT NULL
                                          AND ind.is_voided = false
                                           LEFT JOIN apfodisha.individual_child_growth_monitoring follow_up ON
                                              follow_up.program_enrolment_id = enrl.id
                                          AND follow_up.encounter_date_time IS NOT NULL
                                          AND follow_up.is_voided = false
                                          AND follow_up.last_modified_date_time > :previousCutoffDateTime 
                                          AND follow_up.last_modified_date_time <= :newCutoffDateTime)
UPDATE
    apfodisha.individual_child_growth_monitoring_report growth_report
SET "Severely Underweight"                                          = fld."Severely Underweight",
    "Moderately Underweight"                                        = fld."Moderately Underweight",
    "SAM"                                                           = fld."SAM",
    "MAM"                                                           = fld."MAM",
    "Moderately Stunted"                                            = fld."Moderately Stunted",
    "Severely Stunted"                                              = fld."Severely Stunted",
    "Growth Falter Status (GF-1/GF-2)"                              = fld."Growth Falter Status (GF-1/GF-2)",
    "Height"                                                        = fld."Height",
    "Weight"                                                        = fld."Weight",
    "Date of Measurement"                                           = fld."Date of Measurement",
    "Is the child going to PPK?"                                    = fld."Is the child going to PPK?",
    "Is the child going to Creche?"                                 = fld."Is the child going to Creche?",
    "Is the child being currently exclusively breastfed?"           = fld."Is the child being currently exclusively breastfed?",
    "Is the child being currently breastfed?"                       = fld."Is the child being currently breastfed?",
    "Number of day attended AWC (last month)"                       = fld."Number of day attended AWC (last month)",
    "Is the child receiving egg from AWC"                           = fld."Is the child receiving egg from AWC",
    "Is the child receiving THR from AWC"                           = fld."Is the child receiving THR from AWC",
    "Did the child attended VHND last month"                        = fld."Did the child attended VHND last month",
    "What is the treatment advise for the SAM/MAM/GF2 child?"       = fld."What is the treatment advise for the SAM/MAM/GF2 child?",
    "Is the child enrolled in the CMAM program?"                    = fld."Is the child enrolled in the CMAM program?",
    "Is the child availing benefits (ATHR) under the CMAM program?" = fld."Is the child availing benefits (ATHR) under the CMAM program?",
    "Did you receive additional THR (MSPY)?"                        = fld."Did you receive additional THR (MSPY)?"
FROM growth_monitoring_fields fld
WHERE growth_report."Beneficiary ID" = fld."Individual ID";
