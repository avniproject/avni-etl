-- Full refresh (step 4/5): jnpct_program_exit_view <- jnpct.individual_pregnancy
INSERT INTO jnpct."jnpct_program_exit_view" ("enrolment_date_time", "program_exit_date_time", "date_of_birth", "address_id", "Ind.village", "Ind.Id", "Ind.subcenter", "Ind.phc", "Ind.block", "reason_for_exit", "date_of_death", "reason_of_death", "place_of_death")
SELECT
    e.enrolment_date_time::timestamptz AS "enrolment_date_time",
    e.program_exit_date_time::timestamptz AS "program_exit_date_time",
    i.date_of_birth::timestamp AS "date_of_birth",
    i.address_id::bigint AS "address_id",
    a."Village"::varchar AS "Ind.village",
    i.id::integer AS "Ind.Id",
    a."Subcentre"::varchar AS "Ind.subcenter",
    a."Phc"::varchar AS "Ind.phc",
    a."Block"::varchar AS "Ind.block",
    ex."Reason for exit"::text AS "reason_for_exit",
    ex."Date of death"::timestamp AS "date_of_death",
    ex."Reason of death"::text AS "reason_of_death",
    ex."Place of death"::text AS "place_of_death"
  FROM jnpct."individual_pregnancy" e
  JOIN jnpct.individual i ON i.id = e.individual_id
  LEFT JOIN jnpct."individual_pregnancy_exit" ex ON ex.id = e.id
  LEFT JOIN jnpct.address a ON a.id = i.address_id
  WHERE e.program_exit_date_time IS NOT NULL;
