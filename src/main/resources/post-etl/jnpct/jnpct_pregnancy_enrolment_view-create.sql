-- Shadow table replacing legacy public.jnpct_pregnancy_enrolment_view for Jasper (FD#8512, avni-infra#114).
-- Same name inside the jnpct schema: the Jasper datasource (login user jnpct,
-- default search_path "$user", public) resolves this table instead of the
-- legacy view. Dropping this table instantly reverts Jasper to the legacy view.
set role jnpct;

CREATE TABLE jnpct."jnpct_pregnancy_enrolment_view"
(
    "Ind.Id"                                                   int4    NULL,
    "Ind.address_id"                                           int8    NULL,
    "Ind.uuid"                                                 varchar    NULL,
    "Ind.first_name"                                           varchar    NULL,
    "Ind.last_name"                                            varchar    NULL,
    "Ind.Gender"                                               varchar    NULL,
    "Ind.date_of_birth"                                        date    NULL,
    "Ind.date_of_birth_verified"                               bool    NULL,
    "Ind.registration_date"                                    date    NULL,
    "Ind.facility_id"                                          int8    NULL,
    "Ind.village"                                              varchar    NULL,
    "Ind.subcenter"                                            varchar    NULL,
    "Ind.phc"                                                  varchar    NULL,
    "Ind.block"                                                varchar    NULL,
    "Ind.is_voided"                                            bool    NULL,
    "Enl.Program Name"                                         varchar    NULL,
    "Enl.Id"                                                   int4    NULL,
    "Enl.uuid"                                                 varchar    NULL,
    "Enl.is_voided"                                            bool    NULL,
    "Enl.enrolment_date_time"                                  timestamptz    NULL,
    "Enl.program_exit_date_time"                               timestamptz    NULL,
    "Ind.Marital status"                                       text    NULL,
    "Ind.Number of family members"                             text    NULL,
    "Ind.Who is decision making person in family"              text    NULL,
    "Ind.Ration card"                                          text    NULL,
    "Ind.Caste"                                                text    NULL,
    "Ind.Subcaste"                                             text    NULL,
    "Ind.Religion"                                             text    NULL,
    "Ind.Satipati family"                                      text    NULL,
    "Ind.Addiction"                                            text    NULL,
    "Ind.Addiction - Please specify"                           text    NULL,
    "Ind.Very poor family"                                     text    NULL,
    "Ind.Occupation of mother"                                 text    NULL,
    "Ind.Occupation of husband/father"                         text    NULL,
    "Ind.Mother's education"                                   text    NULL,
    "Ind.Mobile number"                                        text    NULL,
    "Ind.Specialy abled"                                       text    NULL,
    "Ind.Specially abled - Please specify"                     text    NULL,
    "Ind.Any long-term illnesses"                              text    NULL,
    "Ind.Long-term illness - Please specify"                   text    NULL,
    "Ind.Toilet facility present"                              text    NULL,
    "Ind.Using the toilet regularly"                           text    NULL,
    "Ind.Source of drinking water"                             text    NULL,
    "Enl.Last menstrual period"                                date    NULL,
    "Enl.Estimated Date of Delivery"                           date    NULL,
    "Enl.Previous history of disease"                          text    NULL,
    "Enl.Other previous history of disease - Please specify"   text    NULL,
    "Enl.Gravida"                                              text    NULL,
    "Enl.Number of Abortion"                                   text    NULL,
    "Enl.Number of live childrens"                             text    NULL,
    "Enl.MALE"                                                 text    NULL,
    "Enl.FEMALE"                                               text    NULL,
    "Enl.Result of last delivery"                              text    NULL,
    "Enl.Age of Youngest child"                                date    NULL,
    "Enl.Place of last delivery"                               text    NULL,
    "Enl.Risk in the last pregnancy"                           text    NULL,
    "Enl.what kind of risk occurred"                           text    NULL,
    "map"                                                      public.hstore    NULL
);

CREATE UNIQUE INDEX "uidx_pregnancy_enrolment_key" ON jnpct."jnpct_pregnancy_enrolment_view" ("Enl.Id");
CREATE INDEX "idx_pregnancy_enrolment_ind_date_of_birth" ON jnpct."jnpct_pregnancy_enrolment_view" ("Ind.date_of_birth");
CREATE INDEX "idx_pregnancy_enrolment_ind_phc" ON jnpct."jnpct_pregnancy_enrolment_view" ("Ind.phc");
CREATE INDEX "idx_pregnancy_enrolment_enl_enrolment_date_t" ON jnpct."jnpct_pregnancy_enrolment_view" ("Enl.enrolment_date_time");
