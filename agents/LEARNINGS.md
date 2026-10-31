# Learnings

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
