-- Full refresh (step 1/3): every row rebuilt each run (~530 MB across the 11 tables on prerelease, more in production; see README).
-- Runs inside the post-ETL transaction, so readers always see a complete table.
DELETE FROM jnpct."jnpct_child_pnc_view";
