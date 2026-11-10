# Learnings

## 2026-11-10: Historical definitions needed during resource removal

Normal compilation requests today's selectable module catalogue. A deleted node
may still require its exact older definition to recover provider mappings, even
though it generates no module block. Globally current versions need not appear
in an environment's rule-filtered catalogue. This can affect any resource type,
and is not intrinsically a pre-upgrade graph issue.

Treat historical material separately from new version selection: a material-only
lookup must ignore rules and cannot override Environment Pins or adopt restricted
versions. Keep initial provider configurations when merging a second catalogue
read. Persisted-JSON regression tests must populate provider bindings before the
roundtrip so they can assert the original hash and alias survive deletion.

- go-licenses v2.0.1 does not recognize the SDK root EUPL text. Exact declared SDK provenance must be handled explicitly; external unknown licenses remain fatal.
- gitleaks directory scans skip binary files. Stream printable binary strings through the same pinned scanner, without saving extracted secrets.
- Standard-library packages have no Go module record. Dependency notice collection must not treat their package paths as external modules.
- Published golib and graph modules use the same EUPL text. Explicit declarations must enumerate actual module identities, never use an owner-wide exception. Prefix-based classifier exclusions need collision guards.
- A content-only private-reference scan misses sensitive names. Scan relative artifact paths and rescan the complete generated evidence staging tree before publication.
- BuildKit local OCI exports need an explicit image name/tag to populate attestation subjects. An untagged export is not qualified evidence and is not a valid substitute in a tagged reproducibility pair.
- Go's adjacent string pool can create a secret-rule match across benign source literals. Require exact per-file evidence and source/build-metadata binding, never suppress a whole rule or binary class.
- Gitleaks8.30.1 always loads the scan target's .gitleaksignore even with --gitleaks-ignore-path /dev/null. Reject artifact ignore files explicitly, disable inline allow comments and isolate stdin scanner working directories.
- ORAS local-layout digest-only resolution can return application/octet-stream and omit image dependencies. Resolve a descriptor-verified root tag, select the platform and verify its destination manifest and blobs. Unprivileged Syft needs a writable mode1777 temporary filesystem and explicit cache location.
- Parsed tar entries omit arbitrary PAX fields and trailing bytes; gzip decompression also discards optional wrapper fields. Scan retained non-body tar bytes and every validated gzip member's wrapper separately from executable bodies.

## Reciprocal source is more than license text

- go-licenses `save` copies entire source directories for reciprocal libraries.
  Public upstream test credentials and checksum strings can therefore appear
  legitimately under `/licenses`. Classify exact source bytes against verified
  module identities before proposing an exception. A directory-wide exclusion
  would also hide unrelated credentials; pruning source would silently alter
  the approved legal-content policy.
- A redacted Gitleaks report still supplies file/rule/line/column evidence.
  Full-file hashes plus exact finding sets allow narrow classification without
  persisting or printing suspected values.

## Historical migrations and the root functional gate

- A migration 15/16 round trip against the current schema 37 starts from the
  wrong history. Later migrations add checks on `deployments.module_artifacts`;
  dropping that column can remove those checks inside a transaction, so merely
  re-adding the column does not prove the historical migration. Use a fresh
  disposable Goose schema stopped at the target version, assert the migration
  version before and after each Down/Up, and query columns in `current_schema()`.
- `make test-unit` and internal/API tagged tests do not run the root
  `integration-tests` Compose/Kind journeys. CI's separate `integration` job
  runs `make test-integration`, and candidate publication depends on that job.
  A local internal/API pass therefore does not establish root functional or
  packaged release readiness.

## Retained provider-substitution compatibility

A legacy-map test generated with current Go types cannot prove pre-rename Gob compatibility. Frozen synthetic files from an independent old-name encoder reproduce the missing registration. Distinct legacy concrete types avoid duplicate Gob-name registration and normalize to current placeholders before application use. Current canonical writer and dependencies remain unchanged.

## 2026-11-04 retained-history assertion and output retention

A completed Deployment can legitimately lose its encrypted output transfer payload under the existing retention policy. Compare the retained graph and every other historical column, while checking output delivery separately. In the real old-server rehearsal, a retained Runner result reproduced the original whole-row hash through a read-only projection replacing only that ciphertext, proving the cause before changing the assertion.

## 2026-11-04: Go test filters still compile the whole package

Selecting only PostgreSQL-backed API tests still compiles ordinary API test files
and their generated mock imports. A warm developer checkout can hide that missing
job prerequisite. Reproduce on an exact fresh checkout before allocating fixtures.

## Restore executes constraints with a restricted search path

- PostgreSQL restores schema-qualified tables with an empty session search path. CHECK function OIDs can resolve correctly while nested unqualified PL/pgSQL calls fail only for real Draft-containing rows. Test COPY with actual provenance, not only empty/legacy rows.
- Pin nested-validator lookup to pg_catalog and the actual application schema; do not hardcode public, replace validator bodies, or inherit caller-controlled schemas.

## Historical material must survive Runner reconstruction

Graph compilation is only one consumer of immutable material. The real Runner
regression exposed the same current-catalogue policy in inline bundle rebuilding
after compilation was fixed. Reconstruct every already-admitted inline source
through the authenticated material-only catalogue when configured, preserving
all existing artifact and Draft provenance checks. Plan success alone does not
prove asynchronous destroy or rollback execution.

Existing integration fixtures use an internal-system actor. Core deliberately
rejects that actor for delegated admission. Exercise real service authentication
and a scoped human actor in a separate final, tagged integration phase instead
of enabling admission across legacy fixtures.

## Exact advisory metadata corrections

Govulncheck JSON exits zero even when findings exist. Preserve raw output and
enforce symbol-level findings explicitly. A correction must bind the complete
OSV record, scanner identity/protocol and upstream-fixed module version/checksum,
reject replacements and malformed or ambiguous reports, and retain other findings.
Module/package-only findings retain the previous informational policy.

## Portable integration environment projection

Do not compute Score resource names containing `#` inside a shared Make variable.
Escaping that character can pass a literal backslash to Score under Linux Make,
while a different local Make accepts it. Use the existing Bash environment helper
under `-euo pipefail`, followed by `exec "$@"`. Bare assignments propagate Score
failures; `export NAME=$(command)` masks the producer exit status.

Test that actual Make-to-Score boundary with the CI Score version and newly
generated state. A newer local Score state can be unreadable to the pinned older
CLI. Check environment fields without logging credential values before suites.

## Proxy caches survive backend recreation

The Score-generated public Nginx proxy resolves its upstreams while loading
configuration. Recreating Core and DP can swap their addresses while that proxy
continues routing to old addresses. Refresh its configuration after backend
health, then probe a concrete Core route through the public virtual host. Direct
service health alone cannot prove routing. GET `/orgs` is not a Core list method;
a projects-list GET on an unused fixture organization returns200 in Core and404
in DP and provides a safe, nonmutating identity probe.
