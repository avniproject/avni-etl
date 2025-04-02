INSERT INTO apfodisha.individual_child_growth_monitoring_report ("Beneficiary ID",
                                                                 "Name of Child Benefeciaries",
                                                                 "Date of Birth",
                                                                 "Father's name",
                                                                 "Mother's Name")
SELECT ind.id                                     AS "Beneficiary ID",
       concat(ind.first_name, ' ', ind.last_name) AS "Name of Child Beneficiaries",
       ind.date_of_birth                          as "Date of Birth",
       ind."Father/Husband's name"                AS "Father's name",
       ind."Mother's Name"
FROM apfodisha.individual ind
         JOIN
     apfodisha.individual_child enrl ON
                 enrl.individual_id = ind.id
             AND enrl.enrolment_date_time IS NOT NULL
             AND enrl.is_voided = false
         JOIN
     apfodisha.individual_child_growth_monitoring follow_up ON
                 follow_up.program_enrolment_id = enrl.id
             AND follow_up.encounter_date_time IS NOT NULL
             AND follow_up.is_voided = false
WHERE (ind.created_date_time > :previousCutoffDateTime AND ind.created_date_time <= :newCutoffDateTime)
   OR (ind.last_modified_date_time > :previousCutoffDateTime AND ind.last_modified_date_time <= :newCutoffDateTime);
