#!/usr/bin/env sh
set -eu
root="$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$root"
if [ -f scripts/release-tools.sh ]; then
  . ./scripts/release-tools.sh
else
  release_go_version="go$(awk '$1 == "go" { print $2; exit }' go.mod)"
fi
export GOTOOLCHAIN="$release_go_version" GOWORK=off GOENV=off CGO_ENABLED=0
unset GOFLAGS GOEXPERIMENT
checker="$root/scripts/gosecurity/main.go"
case "${1:-source}" in
  source)
    [ "$#" -le 1 ] || exit 2
    go run "$checker" source
    if [ -f shared/go.mod ]; then
      (cd shared && go run "$checker" source)
    fi
    ;;
  binary)
    [ "$#" -eq 2 ] && [ -f "$2" ] || exit 2
    case "$2" in /*) binary="$2" ;; *) binary="$PWD/$2" ;; esac
    # A stripped executable only permits conservative module-level analysis.
    go tool nm "$binary" >/dev/null
    # The checker explicitly enforces symbol findings in the retained JSON report.
    go run "$checker" binary "$binary"
    ;;
  *)
    printf 'usage: check-go-security.sh [source | binary PATH]\n' >&2
    exit 2
    ;;
esac
