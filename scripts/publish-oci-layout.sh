#!/usr/bin/env bash
# Publish one inspected OCI layout root to its tag and prove the registry serves exactly that digest.
#
#   publish-oci-layout.sh LAYOUT_REF IMAGE_TAG IMAGE_DIGEST
#
# `oras cp --no-tty` reads every manifest it has just pushed back from the destination to print its
# status, and GHCR can answer that read with 404 for a moment (Data Plane v4.5.0-rc.3 lost a candidate
# to it). The copy is content-addressed and only ever points IMAGE_TAG at the layout root, so it is
# repeated for exactly that error; oras already retries 408, 429 and 5xx itself. Anything else fails
# at once. A failed attempt may already have written the correct tag, so callers check the
# destination's absence once, before this script, never between attempts.
set -euo pipefail
layout_ref="${1:?usage: publish-oci-layout.sh LAYOUT_REF IMAGE_TAG IMAGE_DIGEST}"
image_tag="${2:?usage: publish-oci-layout.sh LAYOUT_REF IMAGE_TAG IMAGE_DIGEST}"
image_digest="${3:?usage: publish-oci-layout.sh LAYOUT_REF IMAGE_TAG IMAGE_DIGEST}"
# Every failing attempt still lands at least one manifest, so six attempts cover the graphs we publish.
attempts=6
pause="${PUBLISH_RETRY_PAUSE_SECONDS:-10}"
lag='^Error response from registry: sha256:[0-9a-f]{64}: not found$'
errors="$(mktemp)"
trap 'rm -f -- "$errors"' EXIT

for attempt in $(seq 1 "$attempts"); do
  if timeout -k 5s 300s oras cp --no-tty --from-oci-layout "$layout_ref" "$image_tag" 2>"$errors"; then
    break
  else
    status=$?
  fi
  cat "$errors" >&2
  if [ "$status" -ne 1 ] || ! tail -n 1 "$errors" | grep -Eq "$lag" || [ "$attempt" -eq "$attempts" ]; then
    exit "$status"
  fi
  echo "registry did not yet serve a manifest it had just accepted; copy attempt $attempt of $attempts, retrying" >&2
  sleep "$pause"
done

# The same lag can hide the new tag for a moment; any other resolve error is final.
for _ in $(seq 1 12); do
  if digest="$(oras resolve "$image_tag" 2>"$errors")"; then
    if [ "$digest" != "$image_digest" ]; then
      echo "$image_tag resolves to $digest, expected $image_digest" >&2
      exit 1
    fi
    exit 0
  fi
  if ! tail -n 1 "$errors" | grep -Eq ': not found$'; then
    cat "$errors" >&2
    exit 1
  fi
  sleep 5
done
cat "$errors" >&2
echo "$image_tag did not resolve after publication" >&2
exit 1
