# Worklog

## Complete raw tar metadata coverage

- Retain every raw layer header, PAX extension, padding and trailing byte in a
  separate metadata stream, excluding only regular bodies already retained and
  inspected separately. Validate monotonic body offsets and exact body sizes.
  Keep the existing binary-only policy bound to per-file executable evidence.
- Real OCI fixtures with seeded credentials in arbitrary PAX metadata and after
  tar EOF both reproduced the gap before the correction. Mandatory strict scans
  reject both afterward without printing their values. Whiteout coverage remains.

## Immutable BuildKit SBOM generator

- Pinned BuildKit's existing SBOM generator to
  docker/buildkit-syft-scanner@sha256:ae4f3b554449e7e25548e7d8ccc029d17357348e30c6e3df01b92bc93654d6a9
  in both candidate/stable builds and any existing dependency-seed pair.
  Official registry descriptors verify the index and Linux amd64/arm64 manifests.
- Preserve maximum provenance and all SBOM attestations. This existing generator
  is distinct from the unchanged external Syft 1.54.1 image used after inspection.
  Workflow contracts require the exact generator pin and reject mutable sbom:true.
  No build, push, or publication was executed for this tooling correction.

## Exact ORAS installer version validation

- Corrected shell ERE escaping in the Linux ORAS1.3.4 installer. The regression executes the actual installer expression with valid and invalid version output, rejecting wildcard dots and version suffixes without changing approved tool/version/archive checksums.

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

## Exact nullable legal-notice correction

- Reused the reviewed client collector's exact nullable v1.2.0 module checksum, original header hash and authoritative Apache-2.0 supplement hash. Preserve original bytes and record the supplemental source separately; reject replacements, different versions, hash changes and prefix collisions. Other unknown licenses remain fatal.
- The native IAM production graph does not include nullable. DP production does, and CP uses the DP SDK. No exception is silently extended to nullable v1.1.0.

## Strict binary scanner qualification

- Added a source/hash/buildinfo-bound exception for the actual IAM migrator's compiler-adjacent benign validation literals. Keep raw Gitleaks reports only in memory, fail malformed/tool-error/unapproved findings, and report sanitized policy/count only.
- Added real Go ELF and mutation/additional-secret regressions, plus artifact inline-allow and ignore-file suppression regressions. Reject artifact ignore files and isolate stdin scanning. Deduplicate identical raw/merged bytes and batch non-ELF binary strings without exceptions.
- Helper tests cover notice collection and architecture targets unchanged. Production OCI scanning, Syft and independent payload reproducibility are separate evidence and must be recorded from actual runs before qualification.
- Added no-exception relative-path secret scanning. Shared OCI reference validation now protects Syft platform copies and all dormant image publishers from digest-only descriptor/media-type loss; wrong index/platform/media-type regressions fail closed.

## Complete compressed-layer metadata scanning

- Reproduced scanner bypasses in arbitrary PAX metadata and post-tar trailing
  bytes, then retained every non-regular-body tar byte alongside the separately
  inspected original file bodies. Actual seeded credentials now fail closed.
- Reproduced gzip FNAME metadata bypass. Added bounded RFC 1952 member decoding
  with zlib CRC/ISIZE validation, retaining optional fields, fixed headers,
  header checksums, trailers and inter-member padding for strict scanning.
  Legitimate Distroless FNAME layers remain supported.
- Real two-member OCI tests cover independent FNAME, FCOMMENT and FEXTRA
  credentials; malformed members and output/metadata/member bounds fail closed.
  Scanner logs never contain fixture credentials. Actual final artifact rescans
  remain required; helper evidence does not qualify different artifact bytes.
- The new raw stream exposed the public Distroless container home. Exempt only
  exact `/home/nonroot/` from the developer-home pattern, never from secret or
  other concrete scans. Private homes and a credential under that path fail.

## Exact public dependency-source scan policy

- Isolated this change from the frozen release source and private CI controls.
  The unchanged final DP image produced 82 repeated findings from seven exact
  public dependency source files, all matching verified module bytes/checksums.
- Added a complete-file, original-path, module/version/sum and exact-finding-set
  policy. Preserve every source/legal byte and unchanged strict metadata,
  binary, relative-path and private-reference scans. No blanket allowlist.
- Added real pinned-module source fixtures and negatives for changed file,
  source/image module versions, checksum, replacement, relocation/raw entry,
  missing/extra/wrong-rule findings, same-file and unrelated credentials, and
  raw metadata. Fixture credential values never appear in scanner output.
- All 30 content helper tests pass, plus shell syntax, ShellCheck with sourced
  helpers, Python compilation and diff checks. Existing export reinspection
  passes with exactly 82 source findings and zero binary exceptions. A fresh
  multiarch OCI export/check also passed in 74.55 seconds, with report identical
  to the original exact-image export. Independent review is required before use.
- Additional workflow contracts: 18 passed; the pre-existing stable workflow
  contract still expects mutable `sbom: true` instead of the approved pinned
  generator. Its owner was notified; no workflow paths changed in this branch.
