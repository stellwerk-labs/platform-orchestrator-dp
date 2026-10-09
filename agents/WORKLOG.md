# Worklog

## Server artifact foundation

- Added pinned image bases, deterministic Go build settings, architecture-specific /licenses collection, exact SDK source-license provenance, and safe OCI payload inspection/scanning.
- Added helper fixture tests with mandatory pinned go-licenses and gitleaks, including both architectures, changed/missing legal content, binary secrets and whiteouts.
- Fixture/static evidence only. Production builds, hosted suites, public SDK resolution, independent full-image reproducibility and cryptographic attestation verification remain separate required evidence.
- See DECISIONS.md and LEARNINGS.md for the scope and tool limitations.

## Prepublication artifact wiring

- Candidate, stable and existing dependency-seed publishers now build complete local OCI layouts, preserve BuildKit SPDX/provenance, rebuild without instruction cache, inspect/scan both payload sets, compare actual platform bytes and generate pinned offline Syft SPDX before publication.
- ORAS copies the exact inspected layout with unchanged attestation/index bytes. BuildKit itself is digest-pinned; source epoch and gzip/media settings are explicit. Evidence distinguishes platform equality from envelope/index variance.
- Existing human-review environments, repository/event guards, strict suites and duplicate-destination checks remain. No publication was executed as part of implementation.

## Scope and checkout portability corrections

- Restricted the application-prefix exception to its root module and exact same-checkout shared SDK, shipping the actual own SDK license explicitly. Similarly prefixed external modules fail closed. Mandatory helper tests and shell checks passed.
- Made workflow contracts identify the exact component module from go.mod rather than checkout directory name. Added a neutral source-checkout regression and unknown-module rejection, preserving DP publisher and IAM migrator assertions in isolated private CI.
