WITH awc_fields AS (SELECT awc."Project/Block",
                           awc."Sector",
                           awc."AWC",
                           ind.id                   AS "Individual ID"
                    FROM apfodisha.awc_profile ind
                             LEFT JOIN apfodisha.address awc ON
                                awc.id = ind.address_id
                            AND awc.is_voided = false)
UPDATE apfodisha.individual_child_growth_monitoring_report growth_report
SET "Project/Block" = af."Project/Block",
    "Sector"       = af."Sector",
    "AWC"          = af."AWC"
FROM awc_fields af
WHERE growth_report."Beneficiary ID" = af."Individual ID";
