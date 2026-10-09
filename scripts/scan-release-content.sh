#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/release-tools.sh
source "$root/scripts/release-tools.sh"
directory="${1:?usage: scan-release-content.sh DIRECTORY [FORBIDDEN_LITERAL ...]}"
shift
test "$(gitleaks version)" = "$gitleaks_version" || { echo "incorrect gitleaks version" >&2; exit 1; }
python3 "$root/scripts/release_content.py" scan "$directory" "$@"
# Redacted reports remain in process memory. Only exact verified upstream
# reciprocal-source fixtures may pass; all other files and metadata stay strict.
python3 "$root/scripts/release_content.py" source-scan "$directory"
# Gitleaks intentionally skips binary files. Inspect their printable strings as
# a second stream, without writing extracted credentials into logs or reports.
python3 "$root/scripts/release_content.py" binary-scan "$directory"
