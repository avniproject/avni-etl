# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
make build_jar                    # Build JAR without tests
./gradlew clean build             # Full build with tests

# Test
make create-extensions            # One-time prerequisite: create PostgreSQL extensions
make test                         # Run all tests
./gradlew test --tests org.avniproject.etl.SomeTest --stacktrace  # Single test class
./gradlew test --tests org.avniproject.etl.SomeTest.methodName    # Single test method
make open-test-results            # Open HTML test report

# Run
make start                        # bootRun (hot reload via Spring DevTools)
make debug                        # Debug mode on port 5005
make start_server                 # Build JAR then run it
```

Tests require `openchs_test` PostgreSQL database with `uuid-ossp`, `ltree`, and `hstore` extensions.

## Architecture

Avni ETL is a Spring Boot microservice that flattens Avni's hierarchical, RLS-protected PostgreSQL schema into organization-specific flat analytical schemas for reporting. Scheduled Quartz jobs run every 90 minutes (configurable) and sync data incrementally using timestamps.

### ETL Execution Flow (`EtlService`)

For each organization, the ETL runs these phases in order:

1. **Schema Migration** (`SchemaMigrationService`) — Reads form/subject-type metadata from the public schema, computes DDL diffs against the existing org schema, and applies `CREATE TABLE`, `ADD COLUMN`, `DROP COLUMN`, `RENAME COLUMN` etc. Diffs are computed by `SchemaMetadata.findChanges()`.

2. **Data Sync** (`SyncService` → `EntityRepository`) — A chain of 13 `EntitySyncAction` implementations handle different entity types (transactional tables, address hierarchies, repeatable question groups, media, users, approval status, duplicate cleanup, cascade cleanup). Incremental: only rows where `last_modified_date_time` is within the current sync window are processed. Progress tracked in `public.entity_sync_status`.

3. **Reporting Views** (`ReportingViewService`) — Drops and recreates views with address hierarchy joins on every sync (views defined in `reporting_view` table in public schema).

4. **Post-ETL Transformations** (`PostETLSyncService`) — Runs org-specific DDL/DML scripts defined in `src/main/resources/post-etl/{org}/post-etl-sync-processing-config.json`. Runs with the org's DB role.

### Key Design Patterns

**Context Holder** (`OrgIdentityContextHolder`): Thread-local storage of the current org's schema name, DB user, and identity. All repositories use this to set `search_path = {org_schema}, public` before queries — avoid passing org context as parameters.

**Chain of Responsibility** (13 `EntitySyncAction` classes in `repository/sync`): Each handles a specific entity type and is called in sequence by `EntityRepository`. Execution order matters — cascade cleanup must run after transactional sync.

**Table Mapper Pattern** (`domain/tableMappers`): Abstract `Table` class + concrete implementations (`SubjectTable`, `ProgramEnrolmentTable`, `EncounterTable`, etc.). Each defines table name generation and a `Columns` builder for type-specific column sets. Column types: text, integer, date, point, jsonb. Annotation types: index, reportable.

**SQL Template Generation** (`repository/sql`): SQL templates in `src/main/resources/sql/etl/*.sql` use StringTemplate4 (`ST4`) with `${variable}` placeholders (`table_name`, `schema_name`, `subject_type_uuid`, timestamps). `SqlFile` loads and renders templates; `TransactionalSyncSqlGenerator` and `RepeatableQuestionGroupSyncSqlGenerator` build complex queries with conditional blocks (`<if(concept_maps)>...<else>...<endif>`).

**Schema Diff**: Removed form fields → column renamed with `_old` suffix (preserves data). PostgreSQL identifier limit is 63 chars; ETL truncates column names and tracks originals in `ColumnMetadata.original_column_name`. The diff classes live in `domain/metadata`.

### Database Layout

- **`public` schema** — Source tables (`individual`, `program_enrolment`, `encounter`, `program_encounter`, `form`, `concept`, etc.), metadata tables (`table_metadata`, `column_metadata`, `index_metadata`), sync tracking (`entity_sync_status`), and Quartz tables (`quartz_triggers`, `quartz_job_details`).
- **Org schemas** (e.g., `apfodisha`, `rwb`) — Flat analytical tables per org. Named in `organization.schema_name`. JSONB observations expanded to individual columns per concept. Address hierarchy flattened into `address_level_1`, `address_level_2`, … columns (depth varies per org).

### Key Environment Variables

| Variable | Purpose |
|---|---|
| `OPENCHS_DATABASE_*` | DB host, port, name, user, password |
| `AVNI_IDP_TYPE` | Auth provider: `cognito`, `keycloak`, or `none` |
| `AVNI_SCHEDULED_JOB_REPEAT_INTERVAL_IN_MINUTES` | Sync frequency (default 90) |
| `ETL_JOB_THREAD_COUNT` | Quartz thread pool size |
| `OPENCHS_BUCKET_NAME` | S3 bucket for media |

Auth implementations selected by `AVNI_IDP_TYPE`: `KeycloakAuthService`, `IAMAuthService`, `NoIAMAuthService` (dev/test).

### Non-Obvious Constraints

- All tables in one ETL run use `OrgIdentityContextHolder.dataSyncBoundaryTime()` — the cutoff timestamp is fixed at run start to avoid gaps between tables.
- Voiding cascades: encounters cancelled → marked `is_voided=true` → cleanup phase removes them. Program exit cascades to enrolments and encounters. Order in the action chain is critical.
- Views are dropped and recreated every sync run.
- Post-ETL scripts run with the org's DB role, not the `openchs` user — schema grants must be in place.
- Quartz uses a JDBC job store; clustering requires the Quartz tables to exist in the public schema.
