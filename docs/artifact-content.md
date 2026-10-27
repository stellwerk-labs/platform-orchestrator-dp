# Server artifact content

Production images retain the complete digest-pinned Distroless base, including its
certificate store and existing base notices. Application binaries are static Linux
amd64/arm64 builds made with Go 1.26.10, CGO disabled, -trimpath, -buildvcs=false,
GOAMD64=v1 and GOARM64=v8.0. IAM includes both server and authorization-migrate.

## Notice collection

The builder installs go-licenses/v2 v2.0.1 after verifying its reviewed module
checksum through the Go checksum database. scripts/collect-licenses.sh OUTPUT
PACKAGE... requires an explicit GOARCH and follows the union of the supplied
production package graphs for that target. It includes the repository LICENSE,
Go LICENSE/PATENTS, recognized dependency license material and package/module
ancestor LICENSE, COPYING, NOTICE and PATENTS files. /licenses/modules.txt,
target.txt and manifest.sha256 bind the actual target graph and shipped bytes.
Tests and tool dependencies are not substituted for the production graph.

The classifier does not recognize this repository's EUPL text. Only the exact EUPL
module identities in scripts/declared-licenses.tsv have a declared EUPL exception,
bound to reviewed LICENSE bytes and Git blob identity. Published golib and graph module declarations have the same exact-byte checks.
Their dependency graphs
remain checked. Other unknown or absent external licenses fail the build.

For an unpublished SDK, scripts/prepare-sdk-license.py SOURCE_REPO SOURCE_SHA
shared DISPOSABLE_SDK_DIR verifies that every extracted SDK byte matches the exact
source commit, then copies that commit's root LICENSE and emits LICENSE.provenance.
It refuses source-repository destinations, modified snapshots and changed legal
text. Call it only after archive/tree verification, before building the disposable
context. No source checkout or downloaded module cache is modified. Published
SDKs require the actual module-cache LICENSE plus checksum-database module
identity and source commit from Go's download metadata. This is an explicit
provenance declaration, not a wildcard exemption for an organization.

The exact unreplaced nullable v1.2.0 module has a header-only Apache-2.0
LICENSE that this classifier rejects. Its approved correction requires the
compiled module identity/version, module checksum and original header hash.
It preserves that original and adds the hash-pinned authoritative Apache-2.0
text with separate source provenance. Different versions, replacements, changed
bytes and prefix collisions fail; other unknown libraries remain fatal.

## Inspection and reproducibility

scripts/check-server-artifact.sh OCI_LAYOUT NEW_OUTPUT [amd64,arm64] validates
descriptor hashes/sizes, platform configurations, layer diff IDs, ELF architecture,
both IAM executable paths where applicable, all notice hashes and attached
BuildKit SPDX/SLSA subjects. It never materializes untrusted links. Original layer
regular-file bytes and link/path metadata are retained for scanning before
whiteouts, alongside image configurations and attestation statements.

The mandatory pinned gitleaks scanner is redacted and emits no findings report.
Because gitleaks skips binaries, printable strings from NUL-containing files are
also streamed through its stdin scanner. Concrete private paths/owners/URLs, including relative artifact paths, are
checked in binary bytes as well as text. Short planning-ID heuristics apply only
to text to avoid random machine-code collisions. No source-test allowlist is
applied to these server artifacts.

Release/seed callers must supply SOURCE_DATE_EPOCH from the exact source commit
and use BuildKit's image exporter rewrite-timestamp=true. The Dockerfile's epoch
zero default is for ordinary development builds, not release qualification.
The workflows pin the BuildKit multi-platform index as well as the action,
and explicitly select OCI media types and gzip level 6 for both builds.
Retain BuildKit SBOM and mode=max provenance. scripts/generate-server-sbom.sh
adds an offline SPDX scan with the same pinned Syft index as the runtime, for each
actual platform manifest, and checks coverage of the production module graph.
Its namespace and creation time are bound to the platform digest and source epoch.

Run scripts/release_content.py compare FIRST_REPORT SECOND_REPORT after two
independent builds. Equality means identical platform manifests, configurations,
compressed layers, diff IDs, executable bytes and notices. BuildKit attestation
timestamps/invocation metadata, full OCI indexes and signed envelopes can vary.
They are preserved and reported separately, never discarded merely to obtain an
identical index. Presence and subject checks are not a claim of cryptographic
attestation verification.

## Evidence scope

Helper tests use real local module graphs and the mandatory pinned scanners,
including binary-NUL and whiteout-hidden-secret regressions. Passing fixtures is
not evidence that production images, architecture-specific dependency graphs,
native hosted suites or two independent production builds have passed.
Public GOWORK=off dependency resolution remains pending the coordinated SDK tags.

## Binary scanner policy

Artifact-provided ignore files and inline allow comments cannot suppress scans.
The directory scanner remains strict. Printable NUL-containing content is also
scanned, with byte-identical raw/merged files deduplicated, unique ELF files
checked separately, and other binary strings checked without exceptions.

One reviewed binary-only exception covers a generic-api-key false positive
across two compiler-adjacent 21-byte validation literals. It requires the exact
finding hash, both complete literal hashes in the same ELF, source go.sum and
actual Go build metadata for kin-openapi v0.144.0 with its exact module checksum,
no replacement, and Go 1.26.10. Every occurrence must have the verified context.
Other findings, malformed reports, scanner errors and additional credentials in
the same executable fail. Raw finding JSON is retained only in process memory;
output records only the policy identifier and accepted unique-content count.
Artifact relative paths are scanned as a separate stream with no exception.

OCI copies resolve a tag only after its root descriptor matches the inspected
index digest. Syft's platform copy additionally verifies the selected manifest
digest, media type and every referenced blob. Digest-only local-layout
resolution is not used, since it can lose the descriptor's image media type.
