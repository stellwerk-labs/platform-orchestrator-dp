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
