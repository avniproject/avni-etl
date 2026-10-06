-- Full refresh (step 1/2): small tables (<60k rows), rebuilt in <1s each sync.
-- Runs inside the post-ETL transaction, so readers always see a complete table.
DELETE FROM jnpct."jnpct_anc_cluster_incharge_visit_view";
