#!/bin/sh
set -eu
root="$(CDPATH='' cd -- "$(dirname "$0")/.." && pwd)"
cd "$root"
# shellcheck source=scripts/release-tools.sh
. scripts/release-tools.sh
test "${TARGETOS:-}" = linux
case "${TARGETARCH:-}" in amd64|arm64) ;; *) echo "unsupported target architecture" >&2; exit 1 ;; esac
case "${SOURCE_DATE_EPOCH:-0}" in ""|*[!0-9]*) echo "invalid source epoch" >&2; exit 1 ;; esac
export GOTOOLCHAIN="$release_go_version" GOWORK=off GOENV=off CGO_ENABLED=0 GOOS=linux GOARCH="$TARGETARCH" GOAMD64=v1 GOARM64=v8.0
unset GOFLAGS GOEXPERIMENT
mkdir -p /opt/server
go build -trimpath -buildvcs=false -ldflags="-s -w" -o /opt/server/server ./cmd/server
sh scripts/collect-licenses.sh /opt/licenses ./cmd/server
