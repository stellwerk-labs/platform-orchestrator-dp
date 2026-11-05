# Restoring deployment backups containing Draft tests

PostgreSQL `pg_restore` uses a restricted session search path. The Draft CHECK
validators introduced in migration 37 called another validator without its schema.
An archive created before migration 40 containing successful Draft-test Deployments
(schema versions 37–39) can therefore
fail during COPY with `valid_draft_test_reference(jsonb) does not exist`, although
the function itself exists. Empty databases and ordinary rows may hide the defect.

Migration 40 fixes future archives by setting only the two outer functions' search
paths to `pg_catalog`, their actual application schema, then `pg_temp`. No CHECK
is removed, validator body replaced or provenance rule relaxed. New schema 40
archives restore normally without the repair procedure below.

PostgreSQL documents the [archive restore phases](https://www.postgresql.org/docs/18/app-pgrestore.html)
and [per-function settings](https://www.postgresql.org/docs/18/sql-createfunction.html).

## Historical archives created before migration 40

The historical archive itself still contains the old function metadata. Merely
starting a new binary after a failed COPY is insufficient. Preserve the original
archive and its digest. Use a **new, empty, owned target database**, trusted source
roles/globals, the normal PostgreSQL restore tools and the separately retained
platform authorities required by the complete recovery procedure. Preserve a
failed partial target for diagnosis; do not replay this onto it or a running DB.

With connection credentials supplied through the existing private PostgreSQL
service/password configuration, restore only pre-data first:

```sh
pg_restore --exit-on-error --section=pre-data \
  --dbname="$NEW_EMPTY_DATABASE" "$ORIGINAL_ARCHIVE"
```

Confirm the actual DP application schema in the restored schema metadata. It is
normally `public`; installations may select another schema. For **that schema
only**, run the following metadata repair with `psql` identifier quoting:

```sh
psql --set=ON_ERROR_STOP=1 --dbname="$NEW_EMPTY_DATABASE" \
  --set=application_schema="$ACTUAL_DP_SCHEMA" <<'SQL'
BEGIN;
ALTER FUNCTION :"application_schema".valid_draft_test_artifacts(jsonb,uuid)
  SET search_path TO pg_catalog, :"application_schema", pg_temp;
ALTER FUNCTION :"application_schema".valid_draft_test_graph(jsonb,uuid)
  SET search_path TO pg_catalog, :"application_schema", pg_temp;
COMMIT;
SQL
```

Do not run Goose or start DP yet, and do not insert a migration 40 history row
before restoring data. Keep CHECKs, normal restore search-path restrictions and
all other security controls enabled. Continue with the original archive:

```sh
pg_restore --exit-on-error --section=data \
  --dbname="$NEW_EMPTY_DATABASE" "$ORIGINAL_ARCHIVE"
pg_restore --exit-on-error --section=post-data \
  --dbname="$NEW_EMPTY_DATABASE" "$ORIGINAL_ARCHIVE"
```

Compare application rows, deployment provenance, sequences, owners and ACLs with
the backup evidence. Goose history must match the original archive version before
normal migration 40.
This is an explicitly **repaired historical restore**, not a byte-identical vanilla
historical schema restore: the two per-function search-path settings intentionally differ.
After complete data/post-data validation, use the matching forward-compatible DP
migration/startup path to apply 40 normally and verify an idempotent second run.
Do not use an older binary that refuses 40, database Down, disabled constraints,
a global restore search-path override or bulk provenance rewrites as recovery.

Local regression covers empty-search-path COPY, malformed and cross-Environment
Draft rejection, caller-schema shadowing, function metadata preservation, actual
public/custom-schema archives at schema 39, and the separate historical repair
sequence.
Actual platform restoration still requires its own complete archive/authority
validation; a synthetic regression is not that platform's DATA restore receipt.
