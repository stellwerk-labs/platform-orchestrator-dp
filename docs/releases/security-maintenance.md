# Data Plane security maintenance

Unreleased. These notes describe the local maintenance branch, not an available
artifact or a completed deployment.

## Security changes

Official builds use Go 1.26.10, including the certificate verification fix for
[CVE-2026-111943](https://pkg.go.dev/vuln/GO-2026-6809).

Existing HTTP, cryptography, compression, OpenAPI and telemetry dependencies are updated to patched versions. Deployment execution, Module Version resolution, Pins and the plugin boundary keep their existing contracts.

Normal CI now checks source dependencies with pinned govulncheck 1.9.0 and fails
on reachable known vulnerabilities. A passing source scan is not a guarantee
against unknown vulnerabilities or a substitute for deployment qualification.

Integration tests pin the kind node image to Kubernetes 1.36.4 by digest and
install checksum-verified kind and kubectl versions in CI. They no longer follow
an installed kind client's changing default Kubernetes version. The kubeconfig
stays in the owned fixture directory, not the user's current cluster context.

## Compatibility and release checks

Existing published versions and artifacts remain immutable. Use a new patch
version after the normal integration and artifact checks pass. Source builds
following the supported release tooling require Go 1.26.10. Clients using the
corrected vendored API documents must verify their exact published server source
pins before release.
