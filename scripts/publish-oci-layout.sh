#!/usr/bin/env bash
# Publish one inspected OCI layout root to its tag and prove the registry serves exactly that digest.
#
#   publish-oci-layout.sh LAYOUT_REF DESTINATION IMAGE_DIGEST
#
# `oras cp --no-tty` reads every manifest it has just pushed back from the destination to print its
# status, and GHCR can answer that read with 404 for a moment (Data Plane v4.5.0-rc.3 lost a candidate
# to it). The copy is content-addressed and only ever points DESTINATION at the layout root, so it is
# repeated for exactly that error; oras already retries 408, 429 and 5xx itself. Anything else, a
# timeout included, fails at once. A failed attempt may already have written the correct tag, so
# callers check the destination's absence once, before this script, never between attempts.
set -euo pipefail
layout_ref="${1:?usage: publish-oci-layout.sh LAYOUT_REF DESTINATION IMAGE_DIGEST}"
destination="${2:?usage: publish-oci-layout.sh LAYOUT_REF DESTINATION IMAGE_DIGEST}"
image_digest="${3:?usage: publish-oci-layout.sh LAYOUT_REF DESTINATION IMAGE_DIGEST}"
# Every failing attempt still lands at least one manifest, so six attempts cover the published graph.
attempts=6
pause="${PUBLISH_RETRY_PAUSE_SECONDS:-10}"
lag='^Error response from registry: sha256:[0-9a-f]{64}: not found$'
errors="$(mktemp)"
trap 'rm -f -- "$errors"' EXIT

for attempt in $(seq "$attempts"); do
  status=0
  # 6 attempts of 300 s plus pauses must fit the 60 min publisher jobs.
  timeout -k 5s 300s oras cp --no-tty --from-oci-layout "$layout_ref" "$destination" 2>"$errors" || status=$?
  cat "$errors" >&2
  if ((status == 0)); then break; fi
  if ((status != 1 || attempt == attempts)) || ! tail -n 1 "$errors" | grep -Eq "$lag"; then
    exit "$status"
  fi
  printf 'Registry did not yet serve a manifest it had just accepted; copy attempt %d of %d, retrying.\n' \
    "$attempt" "$attempts" >&2
  sleep "$pause"
done

# The new tag can lag behind the copy the same way. Only the remote tag is resolved here, so no layout
# error can reach this pattern and the bare suffix suffices; any other resolve error is final.
tag_missing=': not found$'
# The readback polls every 5 s (12 polls, about a minute), twice as often as the copy repeats; the
# same variable lets the test zero both pauses.
readback_pause="${PUBLISH_RETRY_PAUSE_SECONDS:-5}"
for _ in $(seq 12); do
  if digest="$(oras resolve "$destination" 2>"$errors")"; then
    if [ "$digest" != "$image_digest" ]; then
      echo "$destination resolves to $digest, expected $image_digest" >&2
      exit 1
    fi
    exit 0
  fi
  if ! tail -n 1 "$errors" | grep -Eq "$tag_missing"; then
    cat "$errors" >&2
    exit 1
  fi
  sleep "$readback_pause"
done
cat "$errors" >&2
echo "$destination did not resolve after publication" >&2
exit 1
