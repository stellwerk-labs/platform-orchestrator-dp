#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/release-tools.sh
source "$root/scripts/release-tools.sh"
directory="${1:?usage: scan-release-content.sh DIRECTORY [FORBIDDEN_LITERAL ...]}"
shift
test "$(gitleaks version)" = "$gitleaks_version" || { echo "incorrect gitleaks version" >&2; exit 1; }
python3 "$root/scripts/release_content.py" scan "$directory" "$@"
# Avoid logging finding values, even on scanner failure. No findings report is
# uploaded. Ordinary files have no artifact-wide or source-specific allowlist.
if ! (cd "$directory" && gitleaks dir --redact --no-banner --log-level error --ignore-gitleaks-allow --gitleaks-ignore-path /dev/null --config "$root/scripts/gitleaks.toml" . >/dev/null 2>&1); then
  echo "secret scan failed (finding or scanner error); inspect locally with redaction" >&2
  exit 1
fi
# Gitleaks intentionally skips binary files. Inspect their printable strings as
# a second stream, without writing extracted credentials into logs or reports.
python3 "$root/scripts/release_content.py" binary-scan "$directory"
