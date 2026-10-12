-- Emptied-table check. Run it last in a table's refresh, with the table name as the only sql-params entry
-- (:param1). The service has set search_path to the org schema, so the name resolves there.
-- Fails the whole post-ETL run, so every table keeps its last good data, when a table that had rows
-- comes out of this refresh empty. pg_class.reltuples still holds the estimate from before this run's
-- DELETE; it is -1 (or 0) for a table never analysed, so a first run never trips it.
-- To switch the check off for one table, remove this file and the sql-params entry from that table's config.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ":param1")
        AND (SELECT reltuples FROM pg_class WHERE oid = '":param1"'::regclass) > 0 THEN
        RAISE EXCEPTION 'post-ETL check: %.% had rows before this refresh and is empty after it', current_schema(), ':param1';
    END IF;
END $$;
