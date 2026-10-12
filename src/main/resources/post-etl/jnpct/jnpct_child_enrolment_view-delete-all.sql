-- Full refresh (step 1/2): every row rebuilt each run (~530 MB across the 11 tables on prerelease, more in production; see README).
-- Runs inside the post-ETL transaction, so readers always see a complete table.
DELETE FROM jnpct."jnpct_child_enrolment_view";
