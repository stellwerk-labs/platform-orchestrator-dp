# Decisions

## 2026-11-10: Resolve missing deletion material after graph selection

Fetch only missing deleted-node definitions through the authenticated execution
catalogue with ignored rules and material_only. Attach exact identities after
BuildGraph/AddDeletedNodes, preserving normal module selection and admission.
External plan/forward callers receive a material-specific client so their existing
selection route stays unchanged. Keep deleted inline-source flags false.

Preserve providers from the initial catalogue when historical material contains
the same provider ID. An unavailable exact historical definition is a 409 with
resource/module/version and recovery guidance. Invalid graph configuration is a
400 instead of an internal node dump. Do not skip deleted providers: destruction
still requires them.

- Preserve the complete pinned Distroless base and BuildKit SBOM/provenance.
- Reproducibility compares actual platform payload manifests/configurations/layers/binaries/notices. Attestation and full index variance is recorded, not suppressed.
- Collect production notice graphs separately for each Linux architecture and all shipped executables. Restrict SDK classifier exceptions to exact declared module identities and verified legal bytes/source provenance.
- The application's classifier prefix may cover only its own root module and exact shared SDK from the same source checkout. Other similarly prefixed module identities fail; the own SDK receives the actual source root LICENSE explicitly.
- The sole binary false-positive policy is bound to an exact reviewed finding, adjacent source literal hashes, ELF architecture, dependency version/checksum, no replacement and Go toolchain metadata. Ordinary-file scanning has no such exception, and artifact-owned ignore controls are rejected.

## Exact public reciprocal-source findings

- Preserve go-licenses v2.0.1's complete reciprocal dependency source. Do not
  delete upstream test keys, scanner fixtures or module lockfiles to make a
  secret scan green, and do not suppress entire notice/source directories.
- Permit only seven reviewed HashiCorp source files, bound to their full file
  hashes, original logical image paths, module versions/checksums in both the
  scanner source and image graph, and complete rule/line/column finding sets.
  Replacements, relocations, modified files and additional findings fail.
- Keep directory reports fully redacted in process memory. This source policy
  does not extend to raw metadata, binary strings, filenames or other files.
  Scanner errors and malformed reports remain fatal.

## Automatic historical graph reads

Handle historical Canyon placeholder names at the central provider-substitution decoder for both map and canonical containers. Preserve nested fields and expose current concrete types. Keep existing canonical writes and retained graph rows; no startup history rewrite or schema migration is introduced. Reject corrupt, unknown-type and unsupported-version payloads. Verify existing-environment plan/state/deploy/rollback separately from codec tests.

## 2026-11-04 retained-history assertion and output retention

The retained-graph regression excludes only the policy-managed outputs ciphertext from its immutable-row fingerprint. It still hashes the graph separately and retains all other Deployment fields, Environment UUID and state UID/bytes/lineage checks. No output-retention setting, database row or production behavior changes. The initial failed fixture/case remains preserved; a new private case representation records the verified durable fingerprint.

## 2026-11-04: Integration checkout owns generated test prerequisites

Generate with the existing pinned Make target in the integration job, then check
tracked-file parity before fixture setup. Jobs have independent checkouts; the
verification job's generated mocks cannot satisfy API test-package imports in
integration. Preserve test selection and keep generated mocks uncommitted.

## Migration40: restore-safe Draft validator lookup

- Preserve immutable migration37. ALTER only the two existing outer validator function search_path settings, using deployments-owning namespace, pg_catalog first and pg_temp last. Function identity/body/ACL/owner/volatility and all CHECK acceptance remain unchanged.
- Down40 intentionally retains compatible safety settings. Operational recovery remains forward-only with matching schema-supported binaries; no application/SDK or dependency change.

## Complete retained-material execution path

Retain the exact original Module metadata for removed nodes after new graph
selection. Do not match a replacement or migrate Terraform state: that would
change resource identity without evidence. Defaults, Pins and Rules may omit
historical versions even after versioning, so the repair applies to both migrated
and managed graphs. Missing material is an actionable conflict, never an
automatic reassignment. Current provider definitions win a duplicate merge.

Use authenticated material-only reads for destroy/rollback compilation and
already-admitted Runner bundle reconstruction. These reads grant no mutation
authority; central admission and existing identity/provenance checks remain.
This requires the compatible CP3.3.0/IAM2.6.0/Runner3.2.0 set and normal service
admission identity, rather than a DP-only swap on CP3.1.1. The patch introduces
no migration beyond its DP4.2.1 base; older installations still traverse the
intervening schema changes. Keep the prior historical Gob reader.

## Security fixes needed by the normal build

The unchanged strict artifact build exposed new dependency advisories. Update
existing Echo to4.16.3 and compress to1.20.2. Two Go DB entries still mark fixed
Echo4.16.3 as vulnerable. Independently verified upstream fixes justify only an
exact source/checksum/OSV-hash correction, isolated from graph code. No release
gate is disabled and no additional called finding is accepted. Changed records
that continue reporting a finding require fresh review.

## Fail-fast integration environment boundary

Reuse `local-test-env.sh` for both integration phases, transferring only ports
and cluster context through Make. This removes the second parser's resource
escaping rules. Score-derived values use bare assignments and a grouped export
so lookup failures stop the wrapper. A cheap environment contract runs before
both suites and never prints connection values. No application or protocol
behavior changes.

## Readiness after enabling fixture admission

Reload the original generated public proxy after recreating admission-configured
Core/DP services. Require a unique public Nginx service and successful Core
projects-list GET through its actual Score Host before business creates. Retain
the same authentication and use canonical Compose service names in the separate
admission proxy. These are fixture lifecycle controls, not production routing
changes. Bounded network readiness catches OSError across supported Python
versions and still requires200.

## Installation settings and Provider retirement

- Serialize maintenance changes with durable acceptance. Existing accepted work and exact retries may settle; new records are refused. Retain historical Provider material and require successful canonical state boundaries before authorizing collection.


## 2026-11-12: Data Plane database TLS

- Preserve existing permissions, confirmation and default behavior. PostgreSQL TLS is opt-in through `DATABASE_SSLMODE`; use existing driver and certificate mechanisms.
- Catalogue filters use complete optional summaries, omit version data for core-only readers, and keep unavailable distinct from false. Verification remains unimplemented; its filter is intentionally present with an explicit limitation.
