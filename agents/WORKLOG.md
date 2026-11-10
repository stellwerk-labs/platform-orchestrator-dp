# Worklog

## 2026-11-10: Historical resource removal, local repair

- Based the isolated repair on published DP 4.2.1 (e2f4617), retaining its
  historical placeholder decoder. No API/SDK/dependency/schema change.
- Added post-selection historical material lookup, provider merge preserving the
  initial catalogue, ordinary/external compilation wiring, and clear 409/400 errors.
- Focused no-mock tests cover JSON-roundtripped namespace/database/route deletion,
  provider hash/alias/tfvar retention, unchanged current module selection,
  missing exact version through the HTTP 409 response mapper, and provider merge.
- A separate private real CP 3.3.0/PostgreSQL 17 harness verified the actual resolver
  request flags, deprecated historical material despite a newer Environment Pin,
  no returned rules, unchanged Pin, and missing-version conflict response. It
  exercises material planning, not IAM, full deployment or Runner execution.
- Focused Go tests, make lint, go vet for affected packages, gofmt and diff checks
  passed. Initial fresh-checkout checks needed the existing ignored generated
  mocks; these were generated without adding dependencies. Fresh planning,
  first-green and final independent clean-code reviews accepted five concepts.
- Live incident validation and coordinated artifact publication remain separate.
  No installation change, public push, tag or release occurred.

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

## Local retained-graph compatibility preparation

Implemented distinct historical Gob registrations and central normalization with frozen synthetic independent-encoder fixtures. Exact old source fails the fixtures; the patched graph suite and pinned lint pass. Normal generation then full unit suite passes571 tests; new real-server lifecycle regression compiles and full pinned lint passes. The regression has explicit private phases for an old-to-new same-environment rehearsal; actual predecessor upgrade/artifact execution remains pending. Existing RC5 artifacts are unchanged. No public release or customer mutation is performed by this local commit.

## 2026-11-04 proposed candidate notes

Prepared the exact RC6 notes path required by the normal candidate preflight and consolidated the unreleased retained-graph note there. Existing RC5 notes remain unchanged. This is a local documentation-only successor of the tested compatibility reader; application, fixture, workflow, dependency and canonical writer bytes are unchanged. The original compatibility source/artifact evidence remains bound to its actual source. No tag or publication is created; existing-environment upgrade execution and final-source artifact qualification remain separate gates.

## 2026-11-04 retained-history assertion and output retention

Corrected the new existing-environment regression after the real old-server case exposed normal encrypted-output expiry. Renamed the private case fingerprint to historical_immutable_columns_sha256 and removed only outputs from the SQL JSON projection. Successful output decryption, strict graph/state checks and all other historical columns remain required. Production code, dependencies, migrations, fixtures and candidate notes are unchanged; no public release or completed upgrade is claimed by this test-only change.

## 2026-11-04: Forward integration generation correction

Preserved RC6 and diagnosed its integration setup failure before API cases ran.
Added the existing generation target and tracked parity check to the integration
job, with a workflow order regression and proposed RC7 notes. The correction keeps
all product tests and publication gates. Clean-checkout generation/compile and
source-parity evidence are retained separately by the release coordinator.

## 2026-11-05: candidate Draft backup/restore correction

- Investigated actual custom-archive restore failure and reproduced schema39 empty-search_path COPY failure independently for Draft artifacts and graph. New40 fixes nested lookup without relaxing malformed/cross-Environment checks or caller-schema isolation.
- Added real PostgreSQL migration/metadata/idempotency/Down regression. Historical observation recovery test now explicitly attempts DownTo38 to exercise the migration39 sealed-request fence rather than whichever migration happens to be latest.
- Candidate-only source based on public4d2cc25; no published migration or artifact edited. Qualification receipts, first fixture/tool failures and remaining actual release gates are retained outside the repository.

## 2026-11-10: Complete offline removal qualification

- Added the material-only path for destroy/rollback and Runner inline bundle
  reconstruction. A real asynchronous destroy exposed the second catalogue
  lookup after compilation and plan-only checks had passed.
- Added a tagged real-IAM/Runner regression for migrated opaque and managed
  versions: initial apply, changed Default, unchanged plan-only state bytes,
  failed provider configuration, restored-provider cleanup retry, empty redeploy,
  rollback with immutable original history and successful environment destroy.
- Isolated authenticated fixture bootstrap in a required final CI phase. The
  ordinary integration fixture retains its existing actor behavior; normal
  cleanup removes the added proxy. Raw credentials remain disposable and ignored.
- Full unit suite578, lint, real model/outcome-ack integration, six related
  legacy/lifecycle/pin/provider-removal/destroy tests and30 private captured-graph
  scenarios passed. Customer data and the private replay harness are absent from
  shipping source. Final artifact qualification is recorded separately.
- Updated existing security dependencies and isolated exact stale-advisory
  verification in a standard-library-only scanner wrapper. Focused policy tests
  and source/binary scans passed; final rebuilt-artifact evidence remains distinct.
- Fresh planning, first-green and final clean-code reviews accepted the design.
  Final reviewer counted six production concepts, three fixture concepts and five
  separate security-policy concepts, with no blocking findings.
- See DECISIONS.md for identity/admission boundaries and LEARNINGS.md for the
  asynchronous reconstruction gap. No public push, release or installation write
  is performed by these local changes.

Final local checks: the normal unit gate including scanner-policy cases passes
593tests; lint and8workflow contracts pass. A clean export's normal generation
and both module tidy checks preserve every tracked byte. The normal arm64
Dockerfile build passes dependency verification, binary vulnerability checking
and licence collection. Its executable SHA256 equals the functional-test
executable byte for byte. A subsequent exact-image target startup hit a local
NATS quota problem after Compose regenerated the fixture; the new final target
now explicitly reuses the existing stack and compatible default pins. The
five-minute startup wait was stopped and logged. A further local retry awaits
the user's required timeout check. Hosted CI/multiarchitecture/public-consumer
and live upgrade acceptance remain separate, unexecuted gates.

The user approved the fixture retry. The final maintained target passed against
the normal arm64 image: both legacy and managed variants (59.268s total). The
private fixture retains a fixed4GiB NATS file-store quota for its3GiB reserved
streams; no published product setting changes. Final source security scan also
passes. This completes local qualification; hosted multiarchitecture, publication
and guarded live acceptance remain pending.

## 2026-11-10: Hosted integration environment correction

PR15's first hosted verify gate passed, but integration failed9cases before the
authenticated phase: escaped hashes in the shared Make variable reached Score
as literal resource-name backslashes, leaving database settings empty. Stopped
landing and retained the failed run42792514833; no release tag, GitHub release or public image was published.

Reused the existing Bash helper, removed resource lookups from Make, preserved
argument forwarding and made lookup failure fatal. Added a real environment
contract before both suites. A network-disabled LinuxGNU Make4.4.1 container
using official checksum-verified Score0.17.0 and freshly generated state
reproduces the old failure, passes the new boundary, and proves missing state
stops before the child command. Mac Make3.81 with custom ports also passes.
ShellCheck, Bash syntax, workflow contracts and diff checks pass. This changes
only fixture/control source; qualified application bytes remain unchanged.
See LEARNINGS.md and DECISIONS.md for the portability and fail-fast rationale.

## 2026-11-10: Authenticated-phase proxy lifecycle correction

Second PR CI42798000033 passed verify and the complete ordinary135-test suite.
Its final authenticated cases failed during project creation: retained logs prove
Core POST requests reached DP and returned404 after the fixture recreated both
services. No merge/tag/release/image publication occurred.

Reloaded the generated public Nginx proxy after direct backend health and added
a bounded concrete Core-route probe using Score's virtual Host. Canonical
Compose upstream names remove an implicit alias assumption. A real owned
Core/DP IP swap reproduces public200->404; refresh restoresCore200 while the
same path returns404 from DP. The injected swap disturbed Docker Desktop's
direct port mapping, so both apps were restored through normal Compose before
full business execution. Both historical variants then pass the maintained
target on the normal arm64 image in113.380s. Initial invalid GET/orgs probe and
failed local diagnostic attempts remain private and are not passes.

Fresh planning/first-green reviews accept three localized routing concepts,
with unchanged authentication and no runtime application changes. Source syntax,
workflow contracts and diff checks pass. Normal hosted checks still required.
