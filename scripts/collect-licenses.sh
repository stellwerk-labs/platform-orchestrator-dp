#!/bin/sh
# Collect only production packages for the selected target architecture.
set -eu
root="$(CDPATH='' cd -- "$(dirname "$0")/.." && pwd)"
# shellcheck source=scripts/release-tools.sh
. "$root/scripts/release-tools.sh"
output="${1:?usage: collect-licenses.sh OUTPUT PACKAGE ...}"
shift
test "$#" -gt 0
test ! -e "$output"
case "${GOARCH:-}" in amd64|arm64) ;; *) echo "explicit supported GOARCH required" >&2; exit 1 ;; esac
export GOTOOLCHAIN="$release_go_version" GOWORK=off GOENV=off CGO_ENABLED=0 GOOS=linux GOAMD64=v1 GOARM64=v8.0
unset GOFLAGS GOEXPERIMENT
go version -m "$(command -v go-licenses)" | grep -F "$go_licenses_module" | grep -F "$go_licenses_version" >/dev/null
module="$(go list -m -f '{{.Path}}')"
work="$(mktemp -d)"
trap 'rm -r -- "$work"' EXIT HUP INT TERM
# Only this application's own module is covered by its root license. All
# external dependencies, including replaced SDKs, must have actual notices.
go list -deps -f '{{with .Module}}{{.Path}}|{{.Version}}|{{.Dir}}|{{with .Replace}}{{if .Version}}remote{{else}}local{{end}}{{end}}{{end}}' "$@" > "$work/modules"
sort -u "$work/modules" | sed '/^$/d' > "$work/unique"
ignore="$module"
mkdir "$work/declared"
while IFS='|' read -r dependency version module_dir replacement; do
  case "$dependency" in
    "$module") continue ;;
    "$module/shared"|"$module/shared/v2")
      test "$module_dir" = "$root/shared" || { echo "own SDK must be the source checkout snapshot" >&2; exit 1; }
      continue ;;
    "$module"*) echo "external module collides with application ignore prefix" >&2; exit 1 ;;
  esac
  case "$dependency" in
    github.com/oapi-codegen/nullable)
      test "$version" = v1.2.0 && test -z "$replacement"
      metadata="$(go mod download -json "$dependency@$version")"
      module_sum="$(printf '%s\n' "$metadata" | sed -n 's/^[[:space:]]*"Sum": "\(h1:[^"]*\)"[,]*$/\1/p')"
      sh "$root/scripts/verify-nullable-notice.sh" "$dependency" "$version" "$replacement" "$module_dir" "$module_sum" "$work/nullable"
      ignore="$ignore,$dependency"
      continue ;;
    github.com/oapi-codegen/nullable*) echo "module collides with exact nullable notice correction" >&2; exit 1 ;;
  esac
  declaration="$(awk -F '|' -v module="$dependency" '$1 == module {print}' "$root/scripts/declared-licenses.tsv")"
  test -n "$declaration" || continue
  checksum="$(printf '%s\n' "$declaration" | cut -d '|' -f 3)"
  license_blob="$(printf '%s\n' "$declaration" | cut -d '|' -f 4)"
  test -f "$module_dir/LICENSE" && test ! -L "$module_dir/LICENSE"
  test "$(sha256sum "$module_dir/LICENSE" | awk '{print $1}')" = "$checksum"
  receipt="$work/declared/$(printf '%s' "$dependency" | tr '/' '_')"
  if test "$replacement" = local; then
    test -f "$module_dir/LICENSE.provenance" && test ! -L "$module_dir/LICENSE.provenance"
    grep -F "\"module\": \"$dependency\"" "$module_dir/LICENSE.provenance" >/dev/null
    grep -F "\"licenseSHA256\": \"$checksum\"" "$module_dir/LICENSE.provenance" >/dev/null
    grep -F "\"licenseBlob\": \"$license_blob\"" "$module_dir/LICENSE.provenance" >/dev/null
    grep -F '"spdx": "EUPL-1.2"' "$module_dir/LICENSE.provenance" >/dev/null
    grep -E '"sourceCommit": "[0-9a-f]{40}"' "$module_dir/LICENSE.provenance" >/dev/null
    grep -E '"sourceTree": "[0-9a-f]{40}"' "$module_dir/LICENSE.provenance" >/dev/null
    cp "$module_dir/LICENSE.provenance" "$receipt"
  else
    metadata="$(go mod download -json "$dependency@$version")"
    origin="$(printf '%s\n' "$metadata" | sed -n 's/^[[:space:]]*"Hash": "\([0-9a-f]\{40\}\)"[,]*$/\1/p')"
    archive_sum="$(printf '%s\n' "$metadata" | sed -n 's/^[[:space:]]*"Sum": "\(h1:[^"]*\)"[,]*$/\1/p')"
    test "$(printf '%s' "$origin" | wc -c | tr -d ' ')" = 40 && test -n "$archive_sum"
    printf '{"module": "%s", "version": "%s", "sourceCommit": "%s", "moduleSum": "%s", "licenseBlob": "%s", "licenseSHA256": "%s", "spdx": "EUPL-1.2"}\n' \
      "$dependency" "$version" "$origin" "$archive_sum" "$license_blob" "$checksum" > "$receipt"
  fi
  ignore="$ignore,$dependency"
done < "$work/unique"
# go-licenses uses package prefixes, not module identities. Ensure a declared
# SDK exception cannot silently cover a different module with a similar name.
while IFS='|' read -r dependency version module_dir replacement; do
  if awk -F '|' -v module="$dependency" '$1 == module {found=1} END {exit !found}' "$root/scripts/declared-licenses.tsv"; then continue; fi
  while IFS='|' read -r sdk _spdx checksum license_blob; do
    case "$sdk" in ""|\#*) continue ;; esac
    case "$dependency" in "$sdk") ;; "$sdk"*) echo "external module collides with declared SDK prefix" >&2; exit 1 ;; esac
  done < "$root/scripts/declared-licenses.tsv"
done < "$work/unique"
# Prefixes are limited to this repository and byte-verified, explicitly declared
# SDK modules. Dependencies of ignored packages remain checked by go-licenses.
go-licenses save "$@" --ignore "$ignore" --save_path "$output"
if test -d "$work/nullable"; then
  mkdir -p "$output/github.com/oapi-codegen/nullable"
  cp "$work/nullable/"* "$output/github.com/oapi-codegen/nullable/"
fi
mkdir -p "$output/application" "$output/go"
cp "$root/LICENSE" "$output/application/LICENSE"
goroot="$(go env GOROOT)"
cp "$goroot/LICENSE" "$goroot/PATENTS" "$output/go/"
cut -d '|' -f 1,2 "$work/unique" > "$output/modules.txt"
printf 'linux/%s\n' "$GOARCH" > "$output/target.txt"
# go-licenses saves recognized license texts, but can omit PATENTS/NOTICE.
# Include legal notices from each contributing package through its module root.
go list -deps -f '{{if .Module}}{{.Module.Path}}|{{.Module.Dir}}|{{.Dir}}{{end}}' "$@" > "$work/packages"
while IFS='|' read -r dependency module_dir package_dir; do
  case "$dependency" in ""|"$module") continue ;; esac
  test -d "$module_dir"
  case "$package_dir/" in "$module_dir/"*) ;; *) echo "package outside dependency module" >&2; exit 1 ;; esac
  current="$package_dir"
  while :; do
    for notice in "$current"/LICENSE* "$current"/LICENCE* "$current"/COPYING* "$current"/NOTICE* "$current"/PATENTS*; do
      test -e "$notice" || continue
      test ! -L "$notice" || { echo "symlinked legal notice" >&2; exit 1; }
      test -f "$notice" || continue
      relative="${notice#"$module_dir"/}"
      destination="$output/$dependency/$relative"
      mkdir -p "$(dirname "$destination")"
      if test -e "$destination"; then
        cmp "$notice" "$destination" >/dev/null
      else
        cp "$notice" "$destination"
      fi
    done
    test "$current" != "$module_dir" || break
    current="$(dirname "$current")"
  done
done < "$work/packages"
while IFS='|' read -r dependency version module_dir replacement; do
  receipt="$work/declared/$(printf '%s' "$dependency" | tr '/' '_')"
  case "$dependency" in
    "$module/shared"|"$module/shared/v2")
      mkdir -p "$output/$dependency"
      cp "$root/LICENSE" "$output/$dependency/LICENSE" ;;
  esac
  if test -f "$receipt"; then
    mkdir -p "$output/$dependency"
    cp "$receipt" "$output/$dependency/LICENSE.provenance"
  fi
done < "$work/unique"
# The manifest binds every shipped notice to bytes; no absolute module-cache
# paths are included. Architecture-specific graphs are deliberately separate.
(cd "$output"; find . -type f | LC_ALL=C sort > "$work/files"
 while IFS= read -r notice; do sha256sum "$notice"; done < "$work/files") > "$work/manifest"
mv "$work/manifest" "$output/manifest.sha256"
