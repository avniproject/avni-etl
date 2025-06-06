set role apfodisha;

CREATE TABLE apfodisha.individual_child_growth_monitoring_report_2
(
    "Block"                                                         text    NULL,
    "GP"                                                            text    NULL,
    "Village/Hamlet"                                                text    NULL,
    "Project/Block"                                                 text    NULL,
    "Sector"                                                        text    NULL,
    "AWC"                                                           text    NULL,
    "HH ID"                                                         int4    NULL,
    "Beneficiary ID"                                                int4    NULL,
    "Name of Child Benefeciaries"                                   text    NULL,
    "Date of Birth"                                                 date    NULL,
    "Father's name"                                                 text    NULL,
    "Mother's Name"                                                 text    NULL,
    "Date of Measurement"                                           date    null,
    "Severely Underweight"                                          text    null,
    "Moderately Underweight"                                        text    null,
    "SAM"                                                           text    null,
    "Is it a relapse child (Yes/No)"                                text    null,
    "Was the child facilitated to CHC by QRT"                       text    null,
    "Was the child admitted to NRC before"                          text    null,
    "MAM"                                                           text    null,
    "Moderately Stunted"                                            text    null,
    "Severely Stunted"                                              text    null,
    "Growth Falter Status (GF-1/GF-2)"                              text    null,
    "Height"                                                        numeric NULL,
    "Weight"                                                        numeric NULL,
    "Is the child going to PPK?"                                    text    NULL,
    "Is the child going to Creche?"                                 text    NULL,
    "Is the child being currently exclusively breastfed?"           text    NULL,
    "Is the child being currently breastfed?"                       text    NULL,
    "Number of day attended AWC (last month)"                       numeric NULL,
    "Is the child receiving egg from AWC"                           text    NULL,
    "Is the child receiving THR from AWC"                           text    NULL,
    "Did the child attended VHND last month"                        text    NULL,
    "What is the treatment advise for the SAM/MAM/GF2 child?"       text    NULL,
    "Is the child enrolled in the CMAM program?"                    text    NULL,
    "Is the child availing benefits (ATHR) under the CMAM program?" text    NULL,
    "Did you receive additional THR (MSPY)?"                        text    NULL,
    "Date of Last SAM"                                              date    null,
    "Date of Last MAM"                                              date    null
);
