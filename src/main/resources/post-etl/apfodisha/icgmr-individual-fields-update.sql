UPDATE apfodisha.individual_child_growth_monitoring_report report
SET report."Beneficiary ID"              = ind.id,
    report."Name of Child Benefeciaries" = CONCAT(ind.first_name, ' ', ind.last_name),
    report."Date of Birth"               = ind.date_of_birth,
    report."Father's name"               = ind."Father/Husband's name",
    report."Mother's Name"               = mother.first_name
FROM apfodisha.individual ind
         JOIN apfodisha.individual_child enrl ON
            enrl.individual_id = ind.id
        AND enrl.enrolment_date_time IS NOT NULL
         LEFT JOIN apfodisha.individual_child_growth_monitoring follow_up ON
            follow_up.program_enrolment_id = enrl.id
        AND follow_up.encounter_date_time IS NOT NULL
        AND follow_up.is_voided = false
WHERE ind.is_voided = false
  AND (ind.last_modified_date_time > :previousCutoffDateTime AND ind.last_modified_date_time <= :newCutoffDateTime)
  AND report."Beneficiary ID" = ind.id;
