-- Shadow table replacing legacy public.jnpct_program_exit_view for Jasper (FD#8512, avni-infra#114).
-- Same name inside the jnpct schema: the Jasper datasource (login user jnpct,
-- default search_path "$user", public) resolves this table instead of the
-- legacy view. Dropping this table instantly reverts Jasper to the legacy view.
set role jnpct;

CREATE TABLE jnpct."jnpct_program_exit_view"
(
    "enrolment_date_time"      timestamptz    NULL,
    "program_exit_date_time"   timestamptz    NULL,
    "date_of_birth"            timestamp    NULL,
    "address_id"               int8    NULL,
    "Ind.village"              varchar    NULL,
    "Ind.Id"                   int4    NULL,
    "Ind.subcenter"            varchar    NULL,
    "Ind.phc"                  varchar    NULL,
    "Ind.block"                varchar    NULL,
    "reason_for_exit"          text    NULL,
    "date_of_death"            timestamp    NULL,
    "reason_of_death"          text    NULL,
    "place_of_death"           text    NULL
);

CREATE INDEX "idx_program_exit_ind_phc" ON jnpct."jnpct_program_exit_view" ("Ind.phc");
