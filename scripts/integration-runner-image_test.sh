#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
digest=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

assert_image() {
  local source=$1 expected=$2 actual
  actual=$(printf '%s\n' "print-runner-test-image:;@printf '%s\\n' '\$(RUNNER_TEST_IMAGE)'" | \
    make --no-print-directory -s -C "$repo_dir/integration-tests" -f Makefile -f - \
    print-runner-test-image RUNNER_IMAGE="$source")
  if [[ "$actual" != "$expected" ]]; then
    printf 'source %s: expected %s, got %s\n' "$source" "$expected" "$actual" >&2
    return 1
  fi
}

assert_image "ghcr.io/stellwerk-labs/platform-orchestrator-runner@sha256:$digest" \
  "stellwerk-runner-integration:sha256-$digest"
assert_image "ghcr.io/stellwerk-labs/platform-orchestrator-runner:v3.1.0-rc.1@sha256:$digest" \
  "stellwerk-runner-integration:sha256-$digest"
assert_image "registry.example:5000/runner@sha256:$digest" \
  "stellwerk-runner-integration:sha256-$digest"
assert_image "stellwerk-labs/platform-orchestrator-runner:test" \
  "stellwerk-labs/platform-orchestrator-runner:test"
printf 'Runner integration image references: 4 cases passed\n'
