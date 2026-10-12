# JNPCT — Jasper shadow tables (post-ETL)

**Why:** JNPCT's Jasper reports (reporting-jasper.avniproject.org) query legacy
`public.jnpct_*_view`s directly against OLTP tables on the read replica. At JNPCT's current
volume (~1.19M program encounters) those queries run 30+ minutes and die on replica
recovery conflicts, so every report fails with a generic error
([FD#8512](https://avni.freshdesk.com/a/tickets/8512), avni-infra#114).

**What:** This config rebuilds the same 11 datasets as small tables in the **jnpct schema**,
sourced from the org's ETL tables, on every ETL sync. The tables carry the **same names and
column contracts** as the legacy views. Because the Jasper datasource logs in as DB user
`jnpct` (default `search_path = "$user", public`), the jnpct-schema tables shadow the public
views automatically — **no JRXML changes needed**. Measured: each table rebuilds in <1s
(biggest is ~47k rows); report queries that took 30+ min on the legacy views answer in
seconds against these tables.

## Design notes

- **Full refresh per sync** (`<view>-delete-all.sql` + per-branch inserts), not incremental:
  total rebuild cost is a few seconds, runs inside the post-ETL transaction (readers always
  see a complete table), and is self-healing after any missed sync or schema rebuild.
- **`exists_check` is schema-qualified on every DDL entry.** The default check matches by
  table name across all schemas and would always find the legacy *public view* of the same
  name, so table creation would be skipped forever without it.
- Views that union multiple encounter types (e.g. child PNC + cluster-incharge variant)
  are built as one insert per source table ("branch").
- `jnpct_program_exit_view` has no exposed key column; it unions the exit tables of all four
  JNPCT programs. Non-MCH programs have no death/exit-reason concepts; those columns are NULL
  for them (they were NULL in the legacy view too).

## Deliberate differences from the legacy views

1. **Date-type observations are now correct.** The legacy views cast raw UTC observation
   timestamps to `date`, shifting IST dates one day early (e.g. Date of delivery
   2026-10-02 shown as 2026-10-01). The ETL tables store the correct local date.
2. **Registration columns that were blank are now populated.** The legacy views read some
   individual observations by stale concept UUIDs (e.g. `Ind.Marital status`,
   `Ind.Mother's education` were empty); ETL maps by the current form.
3. **Always-NULL columns stay NULL, now explicitly.** `EncCancel.*` columns were effectively
   always NULL in the legacy views (they read cancel concepts from regular `observations`,
   where they never occur). Five concepts removed from current forms
   (Chiranjivi yojna / government scheme / laboratory test / Burning micturation /
   Ind.Aadhaar card) have no ETL column and are NULL. **No JNPCT report references any of
   these columns** (verified against all 41 JRXMLs).
4. **Data is up to one ETL cycle old.** The legacy views were live. These tables refresh once per
   jnpct ETL run: every 90 minutes by default (`repeatIntervalInMinutes`), plus the run's own time.
   The column `Enc.Counselling Done for the risk factors / Morbidities to all ` keeps the trailing
   space the legacy views gave it.

## Verification after deploy

Every query below returns counts, sizes, timestamps or job status only.

**Before deploy.** Record how long jnpct's last 10 ETL runs took (the job-history query under
*Watching for stale data*, `ended_at - started_at`).

**After the first jnpct run.**
1. The 11 tables exist in the jnpct schema and each has rows (`select count(*)`).
2. Restart the Jasper datasource connection pool. A connection that already ran a report before
   the tables existed can keep reading the legacy views until it is recycled.
3. Run *KMC Care* and *Delivery care of High risk Pregnancy Linelist* end to end. Both should
   return in seconds.

**After three runs.**
1. Compare jnpct's run times with the numbers from before deploy. **Roll back if a run takes more
   than 5 minutes longer.**
2. Table sizes and dead rows, to size what each refresh rewrites:

   ```sql
   select c.relname, pg_size_pretty(pg_total_relation_size(c.oid)) as size, s.n_dead_tup
   from pg_class c join pg_stat_user_tables s on s.relid = c.oid
   where c.relnamespace = 'jnpct'::regnamespace and c.relname like 'jnpct\_%\_view'
   order by pg_total_relation_size(c.oid) desc;
   ```
   If the 11 tables together pass about 1 GB, the full rebuild on every run is worth replacing
   with a refresh of changed rows only.

## Watching for stale data

A failed refresh rolls back every table here. Jasper keeps serving the last good data, with no
error on any report. Two ways to notice:

1. **Job history.** A failed refresh fails the whole jnpct ETL job, and the job history records
   it with `success = false` and the error:

   ```sql
   select started_at, ended_at, success, error_message
   from public.scheduled_job_run
   where job_name = (select uuid from public.organisation where db_user = 'jnpct')
   order by started_at desc limit 10;
   ```
2. **Freshness.** A failure earlier in the ETL (schema migration, sync) never reaches this step,
   so the job history shows that error instead and these tables simply stop moving. The newest
   visit here should be close to the newest in the ETL table it comes from:

   ```sql
   select (select max("Enc.encounter_date_time") from jnpct.jnpct_anc_visit_view) as shadow_newest,
          (select max(encounter_date_time) from jnpct.individual_pregnancy_anc) as etl_newest;
   ```

Anyone changing JNPCT's forms should check the next jnpct run in the job history.

## The emptied-table check

Each table's refresh ends with `../shared/assert-not-emptied.sql`. If a table that had rows comes
out of a refresh empty, the check fails the run, so every table keeps its last good data and the
job history names the table. It never fires on a first run or on a table that was always empty.
It also misses a table that was filled but never analysed, which autovacuum makes rare. To switch
it off for one table, remove the file from that table's `update-sqls` and drop its `sql-params`.

## Other clients that log in as `jnpct`

The shadowing is not specific to Jasper. Any Metabase or Superset query that connects as DB user
`jnpct` and reads `jnpct_*_view` without a schema also gets these tables from now on. avni-server
is not affected: its connections pin `currentSchema=public`.

## Rollback

- **Real rollback (needs a deploy):** rename `post-etl-sync-processing-config.json` to
  `post-etl-sync-processing-config.json_bkp`, as was done for apfodisha in `aed2f73`, deploy,
  then `DROP TABLE jnpct."jnpct_<name>_view";` for all 11 tables. Name resolution falls back
  to the legacy public views.
- **Dropping a table alone is not a rollback.** The DDL pass recreates any missing table on
  every ETL run, so the next jnpct sync (at most `repeatIntervalInMinutes`, 90 by default)
  brings it back. Use it only as a stopgap until the deploy above.
- Full Jasper repository backup (all 41 JNPCT report definitions, pre-change) taken
  2026-10-06 and held by the support team.
- ⚠️ After any jnpct ETL **schema rebuild**, the shadow tables are recreated empty by the
  next sync's DDL pass and refilled by the full-refresh DML in the same run — no manual
  action needed; verify counts once.
