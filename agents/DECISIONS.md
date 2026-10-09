# Decisions

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
