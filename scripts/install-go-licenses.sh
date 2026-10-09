#!/bin/sh
set -eu
root="$(CDPATH='' cd -- "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/release-tools.sh
. "$root/scripts/release-tools.sh"
output="${1:?usage: install-go-licenses.sh OUTPUT_DIRECTORY}"
mkdir -p "$output"
output="$(CDPATH='' cd -- "$output" && pwd)"
export GOTOOLCHAIN="$release_go_version" GOWORK=off GOENV=off GOSUMDB=sum.golang.org
unset GOFLAGS GOEXPERIMENT GOOS GOARCH GOPRIVATE GONOSUMDB GONOPROXY
metadata="$(go mod download -json "$go_licenses_module@$go_licenses_version")"
printf '%s\n' "$metadata" | grep -F '"Sum": "' | grep -F "\"$go_licenses_sum\"" >/dev/null
GOBIN="$output" go install "$go_licenses_module@$go_licenses_version"
go version -m "$output/go-licenses" | grep -F "$go_licenses_module" | grep -F "$go_licenses_version" >/dev/null
