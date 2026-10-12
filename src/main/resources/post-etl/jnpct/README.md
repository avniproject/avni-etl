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

## Deliberate differences from the legacy views (improvements, not regressions)

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

## Verification after deploy

1. After one ETL sync, confirm the 11 tables exist in the jnpct schema and
   `select count(*)` of each is non-zero (except possibly exit view variants).
2. Run the two reports from FD#8512 end-to-end on Jasper:
   *KMC Care* and *Delivery care of High risk Pregnancy Linelist* — both should return
   in seconds.
3. Spot-compare a handful of rows against the legacy view by `"Enc.Id"`
   (expect identical values except the documented date/stale-uuid improvements).

## Failures are silent: watch the ETL log

If any script here fails (for example a form change renames an ETL column), the whole
post-ETL transaction rolls back. The tables keep the last good data and Jasper keeps
serving it, with no error on any report. The only sign is `Error executing post-ETL
scripts` in the ETL log during a jnpct run (the run starts with `Running ETL for` and the
jnpct identity). Anyone changing JNPCT's forms should check the next jnpct ETL run for it.

## Other clients that log in as `jnpct`

The shadowing is not specific to Jasper. Any Metabase or Superset query that connects as
DB user `jnpct` and reads `jnpct_*_view` without a schema also gets these tables from now on,
with the same ~90 min lag and the same improvements listed above.

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
