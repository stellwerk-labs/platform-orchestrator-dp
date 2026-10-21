#!/bin/sh
# Exact approved correction, matching the shared client collector policy.
set -eu
test "$#" = 6
root="$(CDPATH='' cd -- "$(dirname "$0")/.." && pwd)"
module="$1"
version="$2"
replacement="$3"
directory="$4"
sum="$5"
output="$6"
test "$module" = github.com/oapi-codegen/nullable
test "$version" = v1.2.0
test -z "$replacement"
test "$sum" = 'h1:VflFkDW980KhBPiFF7nWSyjg+r4Obqj8lXipV0UkP5w='
header="$directory/LICENSE"
supplement="$root/scripts/release-notices/apache-2.0.txt"
test -f "$header" && test ! -L "$header"
test -f "$supplement" && test ! -L "$supplement"
test "$(sha256sum "$header" | awk '{print $1}')" = d41218ff141de28756b85f79958bc33e077e9bfa6bd695b5e2b8736b779f5a9d
test "$(sha256sum "$supplement" | awk '{print $1}')" = cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30
test ! -e "$output"
mkdir -p "$output"
cp "$header" "$output/LICENSE"
cp "$supplement" "$output/LICENSE-2.0.txt"
printf '%s\n' '{"module":"github.com/oapi-codegen/nullable","version":"v1.2.0","moduleSum":"h1:VflFkDW980KhBPiFF7nWSyjg+r4Obqj8lXipV0UkP5w=","originalSource":"https://raw.githubusercontent.com/oapi-codegen/nullable/v1.2.0/LICENSE","originalSHA256":"d41218ff141de28756b85f79958bc33e077e9bfa6bd695b5e2b8736b779f5a9d","supplementSource":"https://www.apache.org/licenses/LICENSE-2.0.txt","supplementSHA256":"cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30","reason":"Original copyright and header preserved. Authoritative full Apache-2.0 license supplied without modifying dependency bytes."}' > "$output/notice-provenance.json"
