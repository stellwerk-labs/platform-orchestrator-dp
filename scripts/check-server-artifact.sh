#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
layout="${1:?usage: check-server-artifact.sh OCI_LAYOUT NEW_OUTPUT [amd64,arm64]}"
output="${2:?new inspection output required}"
architectures="${3:-amd64,arm64}"
python3 "$root/scripts/release_content.py" export "$layout" "$output" "$architectures" \
  github.com/stellwerk-labs/platform-orchestrator-dp opt/server/server
bash "$root/scripts/scan-release-content.sh" "$output" "${RELEASE_FORBIDDEN_LITERAL:-}"
