 with addresses AS
  (SELECT i.id AS "Individual ID",
          village."Block" AS "Block",
          village."GP" AS "GP",
          village."Village/Hamlet" AS "Village/Hamlet",
          awc."Project/Block" AS "Project/Block",
          awc."Sector" AS "Sector",
          awc."AWC" AS "AWC",
          house."id" AS "HH ID"
   FROM apfodisha.individual i
   LEFT JOIN apfodisha.address village ON village.id = i."address_id"
   AND village.is_voided = FALSE
   LEFT JOIN public.group_subject gs ON gs.member_subject_id = i.id
   AND gs.is_voided = FALSE
   LEFT JOIN apfodisha.household house ON gs.group_subject_id = house.id
   AND house.is_voided = FALSE
   LEFT JOIN apfodisha.address awc ON awc.uuid = house."AWC Name"
   AND awc.is_voided = FALSE )
   UPDATE apfodisha.individual_child_growth_monitoring_report growth_report
   SET "Project/Block"  = af."Project/Block",
       "Sector"         = af."Sector",
       "AWC"            = af."AWC",
       "Block"          = af."Block",
       "GP"             = af."GP",
       "Village/Hamlet" = af."Village/Hamlet",
       "HH ID"          = af."HH ID"
   FROM addresses af
   WHERE growth_report."Beneficiary ID" = af."Individual ID";