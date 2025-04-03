WITH village_fields AS (SELECT village."Block",
                               village."GP",
                               village."Village/Hamlet",
                               ind.id                   AS "Individual ID"
                        FROM apfodisha.individual ind
                                 LEFT JOIN apfodisha.address village ON
                                    village.id = ind.address_id
                                )
UPDATE apfodisha.individual_child_growth_monitoring_report growth_report
SET "Block"          = vf."Block",
    "GP"            = vf."GP",
    "Village/Hamlet" = vf."Village/Hamlet"
FROM village_fields vf
WHERE growth_report."Beneficiary ID" = vf."Individual ID";
