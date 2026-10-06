-- Full refresh (step 1/3): small tables (<60k rows), rebuilt in <1s each sync.
-- Runs inside the post-ETL transaction, so readers always see a complete table.
DELETE FROM jnpct."jnpct_child_pnc_view";
