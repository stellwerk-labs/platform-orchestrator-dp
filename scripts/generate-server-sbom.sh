#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=scripts/release-tools.sh
source "$root/scripts/release-tools.sh"
layout="${1:?usage: generate-server-sbom.sh LAYOUT INSPECTION_OUTPUT NEW_SBOM_OUTPUT}"
inspection="${2:?inspection output required}"
output="${3:?new SBOM output required}"
test ! -e "$output"
mkdir "$output"
case "${SOURCE_DATE_EPOCH:-}" in ""|*[!0-9]*) echo "explicit source epoch required" >&2; exit 1 ;; esac
case "$(docker info --format '{{.OSType}}/{{.Architecture}}')" in
  linux/amd64|linux/x86_64) platform=linux/amd64 ;;
  linux/arm64|linux/aarch64) platform=linux/arm64 ;;
  *) echo "unsupported scanner Docker architecture" >&2; exit 1 ;;
esac
timeout -k 5s 120s docker pull --quiet --platform "$platform" "$syft_image" >/dev/null
docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$syft_image" | grep -Fx "$syft_image" >/dev/null
reference="$(python3 "$root/scripts/release_content.py" oci-reference "$layout" "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["index"])' "$inspection/report.json")")"
while IFS='|' read -r arch manifest; do
  image="$output/image-$arch"
  timeout -k 5s 120s oras cp --no-tty --platform "linux/$arch" --from-oci-layout --to-oci-layout \
    "$layout:$reference" "$image:scan" >/dev/null
  python3 "$root/scripts/release_content.py" sbom-layout "$image" "$manifest"
  timeout -k 5s 240s docker run --rm --pull=never --platform "$platform" --network none --read-only \
    --user "$(id -u):$(id -g)" --cap-drop ALL --security-opt no-new-privileges \
    --tmpfs /tmp:rw,noexec,nosuid,size=128m,mode=1777 --env HOME=/tmp --env XDG_CACHE_HOME=/tmp/.cache --env SYFT_CHECK_FOR_APP_UPDATE=false \
    --mount "type=bind,src=$image,dst=/image,readonly" \
    "$syft_image" scan oci-dir:/image -o spdx-json > "$output/$arch.spdx.json"
  python3 - "$output/$arch.spdx.json" "$inspection/$arch/licenses/modules.txt" "$manifest" "$SOURCE_DATE_EPOCH" "$arch" <<'PY'
import datetime, json, pathlib, sys
path, modules, manifest, epoch, arch = sys.argv[1:]
document = json.loads(pathlib.Path(path).read_text())
if document.get("spdxVersion") != "SPDX-2.3":
    raise SystemExit("expected SPDX 2.3")
packages = document.get("packages", [])
names = {package.get("name") for package in packages}
expected = {line.split("|")[0] for line in pathlib.Path(modules).read_text().splitlines()}
if not expected <= names or not any(package.get("name") == "stdlib" and package.get("versionInfo") == "go1.26.2" for package in packages):
    raise SystemExit("SBOM does not cover production Go graph and pinned Go distribution")
document["name"] = "server-linux-" + arch
document["documentNamespace"] = "https://stellwerk.dev/sbom/" + manifest.replace(":", "/") + "/linux/" + arch
document["creationInfo"]["created"] = datetime.datetime.fromtimestamp(int(epoch), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
pathlib.Path(path).write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
PY
done < <(python3 - "$inspection/report.json" <<'PY'
import json, sys
for arch, platform in sorted(json.load(open(sys.argv[1]))["payloads"].items()):
    print(arch + "|" + platform["manifest"])
PY
)
bash "$root/scripts/scan-release-content.sh" "$output" "${RELEASE_FORBIDDEN_LITERAL:-}"
