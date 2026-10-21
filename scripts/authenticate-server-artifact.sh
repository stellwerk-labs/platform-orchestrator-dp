#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/publication-tools.sh
source "$root/scripts/publication-tools.sh"
layout="${1:?usage: authenticate-server-artifact.sh INSPECTED_LAYOUT STAGED_EVIDENCE NEW_VERIFICATION_OUTPUT}"
evidence="${2:?staged evidence required}"
output="${3:?new verification output required}"
export DOCKER_CONFIG="${DOCKER_CONFIG:-$HOME/.docker}"
test -d "$DOCKER_CONFIG"
python3 "$root/scripts/server-publication.py" verify "$layout" "$evidence"
bash "$root/scripts/scan-release-content.sh" "$evidence" "${RELEASE_FORBIDDEN_LITERAL:-}"
test ! -e "$output"
mkdir "$output"
test "$(oras resolve "$IMAGE_TAG")" = "$IMAGE_DIGEST"
timeout -k 5s 120s docker pull --quiet "$cosign_image" >/dev/null
docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$cosign_image" | grep -Fx "$cosign_image" >/dev/null
verification=(--certificate-identity "https://github.com/$GITHUB_WORKFLOW_REF"
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
  --certificate-github-workflow-sha "$GITHUB_WORKFLOW_SHA")
cosign_release() {
  timeout -k 5s 120s docker run --rm --pull=never --read-only --cap-drop ALL \
    --security-opt no-new-privileges --user "$(id -u):$(id -g)" \
    --tmpfs /tmp:rw,noexec,nosuid,size=32m,mode=1777 --env HOME=/tmp --env DOCKER_CONFIG=/docker \
    --env GITHUB_ACTIONS --env ACTIONS_ID_TOKEN_REQUEST_URL --env ACTIONS_ID_TOKEN_REQUEST_TOKEN \
    --mount "type=bind,src=$DOCKER_CONFIG,dst=/docker,readonly" \
    --mount "type=bind,src=$evidence,dst=/evidence,readonly" \
    --mount "type=bind,src=$output,dst=/verification" "$cosign_image" "$@"
}
image="${IMAGE_TAG%:*}"
subjects="$(jq -er '.index, (.payloads | to_entries[] | .value.manifest)' "$evidence/platforms.json")"
platforms="$(jq -er '.payloads | to_entries[] | [.key, .value.manifest] | join("|")' "$evidence/platforms.json")"
while IFS= read -r subject; do
  cosign_release sign --yes --oidc-provider github-actions "$image@$subject" >/dev/null
  cosign_release verify "${verification[@]}" "$image@$subject" > "$output/${subject#sha256:}.signature.json"
  cosign_release attest --yes --oidc-provider github-actions --type slsaprovenance1 \
    --predicate /evidence/source-provenance.json "$image@$subject" >/dev/null
  proof="$output/${subject#sha256:}.provenance.json"
  cosign_release verify-attestation "${verification[@]}" --type slsaprovenance1 --output json "$image@$subject" > "$proof"
  python3 "$root/scripts/server-publication.py" statement "$proof" "$subject" \
    https://slsa.dev/provenance/v1 "$evidence/source-provenance.json"
done <<< "$subjects"
while IFS='|' read -r arch platform; do
  for subject in "$IMAGE_DIGEST" "$platform"; do
    cosign_release attest --yes --oidc-provider github-actions --type spdxjson \
      --predicate "/evidence/$arch.spdx.json" "$image@$subject" >/dev/null
    proof="$output/${subject#sha256:}.$arch.spdx.json"
    cosign_release verify-attestation "${verification[@]}" --type spdxjson --output json "$image@$subject" > "$proof"
    python3 "$root/scripts/server-publication.py" statement "$proof" "$subject" \
      https://spdx.dev/Document "$evidence/$arch.spdx.json"
  done
done <<< "$platforms"
cosign_release sign-blob --yes --oidc-provider github-actions \
  --bundle /verification/SHA256SUMS.sigstore.json /evidence/SHA256SUMS >/dev/null
cosign_release verify-blob "${verification[@]}" \
  --bundle /verification/SHA256SUMS.sigstore.json /evidence/SHA256SUMS >/dev/null
bash "$root/scripts/scan-release-content.sh" "$output" "${RELEASE_FORBIDDEN_LITERAL:-}"
anonymous="$(mktemp -d "$RUNNER_TEMP/server-anonymous.XXXXXX")"
trap 'rmdir "$anonymous"' EXIT
test "$(DOCKER_CONFIG="$anonymous" oras resolve "$IMAGE_TAG")" = "$IMAGE_DIGEST"
# Every anonymous check remains cryptographic and keeps the same exact identity.
while IFS= read -r subject; do
  DOCKER_CONFIG="$anonymous" cosign_release verify "${verification[@]}" "$image@$subject" >/dev/null
  proof="$output/${subject#sha256:}.anonymous-provenance.json"
  DOCKER_CONFIG="$anonymous" cosign_release verify-attestation "${verification[@]}" \
    --type slsaprovenance1 --output json "$image@$subject" > "$proof"
  python3 "$root/scripts/server-publication.py" statement "$proof" "$subject" \
    https://slsa.dev/provenance/v1 "$evidence/source-provenance.json"
done <<< "$subjects"
while IFS='|' read -r arch platform; do
  for subject in "$IMAGE_DIGEST" "$platform"; do
    proof="$output/${subject#sha256:}.$arch.anonymous-spdx.json"
    DOCKER_CONFIG="$anonymous" cosign_release verify-attestation "${verification[@]}" \
      --type spdxjson --output json "$image@$subject" > "$proof"
    python3 "$root/scripts/server-publication.py" statement "$proof" "$subject" \
      https://spdx.dev/Document "$evidence/$arch.spdx.json"
  done
done <<< "$platforms"
test "$(DOCKER_CONFIG="$anonymous" oras resolve "$IMAGE_TAG")" = "$IMAGE_DIGEST"
bash "$root/scripts/scan-release-content.sh" "$output" "${RELEASE_FORBIDDEN_LITERAL:-}"
