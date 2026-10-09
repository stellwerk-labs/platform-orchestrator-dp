#!/usr/bin/env python3
"""Inspect actual server OCI payloads without materializing untrusted links."""
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def blob(layout, descriptor):
    value, size = descriptor.get("digest", ""), descriptor.get("size")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", value) or type(size) is not int or size < 0:
        raise ValueError("invalid OCI descriptor")
    path = layout / "blobs" / "sha256" / value[7:]
    if (layout / "blobs").is_symlink() or path.parent.is_symlink() or path.is_symlink() or not path.is_file():
        raise ValueError("missing or symlinked OCI blob")
    data = path.read_bytes()
    if len(data) != size or digest(data) != value:
        raise ValueError("OCI blob digest or size mismatch")
    return data


def safe_name(name):
    path = PurePosixPath(name)
    normalized = str(path)
    if path.is_absolute() or ".." in path.parts or not path.parts or "\\" in name:
        raise ValueError("unsafe OCI layer path")
    return normalized


def notices(files, arch, own_module):
    required = ("application/LICENSE", "go/LICENSE", "go/PATENTS", "modules.txt", "target.txt", "manifest.sha256")
    bundle = {name[9:]: value for name, value in files.items() if name.startswith("licenses/")}
    for name in required:
        if name not in bundle or not isinstance(bundle[name], bytes) or not bundle[name].strip():
            raise ValueError("missing required license/graph notice")
    if bundle["target.txt"] != ("linux/" + arch + "\n").encode():
        raise ValueError("license graph target differs from image architecture")
    listed = {}
    for line in bundle["manifest.sha256"].decode().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  \./(.+)", line)
        if not match or match[2] in listed:
            raise ValueError("invalid license manifest")
        listed[match[2]] = match[1]
    actual = {name: hashlib.sha256(data).hexdigest() for name, data in bundle.items() if name != "manifest.sha256" and isinstance(data, bytes)}
    if listed != actual or len(bundle) != len(actual) + 1:
        raise ValueError("license manifest does not cover exactly the shipped regular files")
    modules = bundle["modules.txt"].decode().splitlines()
    if not modules or modules != sorted(set(modules)):
        raise ValueError("invalid production dependency graph")
    for line in modules:
        module, version = line.split("|")
        if not module or any(character.isspace() for character in module + version):
            raise ValueError("invalid production module identity")
        # The root EUPL explicitly covers this repository, including its SDK.
        if module == own_module:
            continue
        if module in (own_module + "/shared", own_module + "/shared/v2"):
            if bundle.get(module + "/LICENSE") != bundle["application/LICENSE"]:
                raise ValueError("own SDK lacks its source repository license")
            continue
        candidates = [name for name in bundle if name.startswith(module + "/") and
                      re.match(r"(?i)(LICENSE|LICENCE|COPYING)", PurePosixPath(name).name)]
        if not candidates:
            raise ValueError("production dependency has no shipped license")
    return {name: digest(data) for name, data in sorted(bundle.items())}


def export(layout, output, architectures, own_module, binaries):
    layout, output = Path(layout), Path(output)
    if output.exists() or layout.is_symlink():
        raise ValueError("inspection output must be new")
    if (layout / "oci-layout").is_symlink() or (layout / "index.json").is_symlink():
        raise ValueError("symlinked OCI metadata")
    if json.loads((layout / "oci-layout").read_text()) != {"imageLayoutVersion": "1.0.0"}:
        raise ValueError("unsupported OCI layout")
    index = json.loads((layout / "index.json").read_text())
    if len(index["manifests"]) != 1:
        raise ValueError("layout must contain one release index")
    top = index["manifests"][0]
    image_index = json.loads(blob(layout, top))
    manifests = image_index.get("manifests")
    if image_index.get("schemaVersion") != 2 or not isinstance(manifests, list):
        raise ValueError("release must be an OCI image index")
    payloads = {}
    attestations = []
    output.mkdir()
    metadata = output / "metadata"
    metadata.mkdir()
    (metadata / "index.json").write_text(json.dumps(image_index, sort_keys=True))
    for descriptor in manifests:
        platform = descriptor.get("platform", {})
        manifest = json.loads(blob(layout, descriptor))
        config_bytes = blob(layout, manifest["config"])
        config = json.loads(config_bytes)
        identity = descriptor["digest"][7:]
        (metadata / (identity + ".json")).write_text(json.dumps({"descriptor": descriptor, "manifest": manifest, "config": config}, sort_keys=True))
        if platform.get("os") == "unknown" and platform.get("architecture") == "unknown":
            if descriptor.get("annotations", {}).get("vnd.docker.reference.type") != "attestation-manifest":
                raise ValueError("unknown non-attestation platform")
            reference = descriptor["annotations"].get("vnd.docker.reference.digest", "")
            predicates = set()
            for number, layer in enumerate(manifest["layers"]):
                data = blob(layout, layer)
                if layer["mediaType"] != "application/vnd.in-toto+json":
                    raise ValueError("unexpected attestation layer")
                statement = json.loads(data)
                if statement.get("_type") not in ("https://in-toto.io/Statement/v0.1", "https://in-toto.io/Statement/v1"):
                    raise ValueError("unsupported attestation statement")
                predicates.add(statement.get("predicateType"))
                if not any(subject.get("digest", {}).get("sha256") == reference.removeprefix("sha256:")
                           for subject in statement.get("subject", [])):
                    raise ValueError("attestation subject does not bind platform manifest")
                (metadata / (identity + "-" + str(number) + ".json")).write_bytes(data)
            if "https://spdx.dev/Document" not in predicates or not predicates.intersection(
                    {"https://slsa.dev/provenance/v0.2", "https://slsa.dev/provenance/v1"}):
                raise ValueError("platform lacks SPDX SBOM or SLSA provenance")
            attestations.append(descriptor["digest"])
            continue
        arch = platform.get("architecture")
        if platform.get("os") != "linux" or arch not in architectures or arch in payloads:
            raise ValueError("unexpected or duplicate platform")
        image_config = config.get("config", {})
        if (config.get("os"), config.get("architecture")) != ("linux", arch):
            raise ValueError("image config architecture mismatch")
        if image_config.get("Entrypoint") != ["/opt/server/server"] or image_config.get("User") not in ("65532", "65532:65532", "nonroot"):
            raise ValueError("unexpected server entrypoint or user")
        files, modes, diff_ids, layers = {}, {}, [], []
        for number, layer in enumerate(manifest["layers"]):
            data = blob(layout, layer)
            media = layer["mediaType"]
            if media in ("application/vnd.oci.image.layer.v1.tar+gzip", "application/vnd.docker.image.rootfs.diff.tar.gzip"):
                raw = gzip.decompress(data)
            elif media == "application/vnd.oci.image.layer.v1.tar":
                raw = data
            else:
                raise ValueError("unsupported image layer compression")
            diff_ids.append(digest(raw))
            layers.append(layer["digest"])
            raw_root = output / "layers" / arch / str(number)
            raw_root.mkdir(parents=True)
            entries, seen = [], set()
            with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
                for entry_number, entry in enumerate(archive):
                    if entry.isdir() and entry.name in (".", "./"):
                        continue
                    name = safe_name(entry.name)
                    if name in seen:
                        raise ValueError("duplicate layer entry")
                    seen.add(name)
                    record = {"name": name, "mode": entry.mode, "type": entry.type.decode("ascii"), "link": entry.linkname}
                    entries.append(record)
                    path = PurePosixPath(name)
                    if path.name.startswith(".wh."):
                        if not entry.isfile() or entry.size:
                            raise ValueError("invalid whiteout")
                        if path.name == ".wh..wh..opq":
                            removed = "" if str(path.parent) == "." else str(path.parent) + "/"
                            files = {key: value for key, value in files.items() if not key.startswith(removed)}
                        else:
                            removed = str(path.with_name(path.name[4:]))
                            files = {key: value for key, value in files.items() if key != removed and not key.startswith(removed + "/")}
                    elif entry.isfile():
                        content = archive.extractfile(entry).read()
                        # Retain original bytes, including files hidden by later whiteouts.
                        (raw_root / str(entry_number)).write_bytes(content)
                        files[name] = content
                        modes[name] = entry.mode
                    elif entry.isdir():
                        files.pop(name, None)
                        continue
                    elif entry.issym() or entry.islnk():
                        # Never create attacker-controlled links on the host. Their
                        # names/targets remain in scanned metadata; required payloads
                        # below must resolve to actual regular files, not links.
                        files[name] = None
                    else:
                        raise ValueError("unsupported non-file OCI entry")
            (raw_root / "entries.json").write_text(json.dumps(entries, sort_keys=True))
        if config.get("rootfs") != {"type": "layers", "diff_ids": diff_ids}:
            raise ValueError("layer diff ID mismatch")
        binary_hashes = {}
        destination = output / arch
        for name in list(binaries) + [name for name in files if name.startswith("licenses/")]:
            if any(str(parent) in files for parent in PurePosixPath(name).parents):
                raise ValueError("required payload has a non-directory ancestor")
        for name in binaries:
            content = files.get(name)
            machine = 62 if arch == "amd64" else 183
            if not isinstance(content, bytes) or not modes.get(name, 0) & 0o111 or len(content) < 20 or content[:6] != b"\x7fELF\x02\x01" or int.from_bytes(content[18:20], "little") != machine:
                raise ValueError("missing or wrong-architecture server executable")
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(content)
            binary_hashes[name] = digest(content)
        license_hashes = notices(files, arch, own_module)
        for name, content in files.items():
            if name.startswith("licenses/") and isinstance(content, bytes):
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
        payloads[arch] = {"manifest": descriptor["digest"], "config": manifest["config"]["digest"],
                          "layers": layers, "diffIds": diff_ids, "binaries": binary_hashes, "licenses": license_hashes}
    if set(payloads) != set(architectures):
        raise ValueError("missing required image platform")
    # SBOM/provenance stay attached. Presence is checked here; their predicates
    # are scanned but are not claimed cryptographically verified by this helper.
    subjects = {d.get("annotations", {}).get("vnd.docker.reference.digest") for d in manifests
                if d.get("annotations", {}).get("vnd.docker.reference.type") == "attestation-manifest"}
    if subjects != {value["manifest"] for value in payloads.values()}:
        raise ValueError("missing platform BuildKit attestations")
    report = {"schemaVersion": 1, "index": top["digest"], "payloads": payloads, "attestations": sorted(attestations),
              "reproducibilityBoundary": "Compare platform manifests/configs/layers/binaries/notices. Attestations and full index may vary; they are preserved, not removed."}
    (output / "report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def compare(first, second):
    a, b = (json.loads(Path(path).read_text()) for path in (first, second))
    if a["payloads"] != b["payloads"]:
        raise ValueError("platform payload reproducibility mismatch")
    print(json.dumps({"platformPayloadsIdentical": True, "fullIndexIdentical": a["index"] == b["index"],
                      "attestationsIdentical": a["attestations"] == b["attestations"]}, sort_keys=True))


def scan(directory, forbidden=()):
    root = Path(directory)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("missing or symlinked scan input")
    common = [rb"/(?:Users|home)/[A-Za-z0-9_.-]+/", rb"github\.com/[^\s\"'<>]+-enterprise\b",
              rb"\bhttps?://[a-z0-9.-]+\.(?:internal|localdomain)\b"] + [re.escape(value.encode()) for value in forbidden if value]
    text = [rb"\bWX[0-9]+\b", rb"\b(?:S1\.[0-9]+|PM[0-9]{2}|MO[0-9]{2}|[QCD][0-9]{2})\b"]
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix().encode()
        if any(re.search(pattern, relative) for pattern in common + text):
            raise ValueError("private reference scan found prohibited content")
        if path.is_symlink():
            raise ValueError("refusing symlinked scan content")
        if path.is_file():
            data = path.read_bytes()
            if any(re.search(pattern, data) for pattern in common + ([] if b"\0" in data else text)):
                raise ValueError("private reference scan found prohibited content")


def binary_strings(directory):
    root = Path(directory)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("missing or symlinked scan input")
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError("refusing symlinked scan content")
        if path.is_file():
            data = path.read_bytes()
            if b"\0" in data:
                for value in re.findall(rb"[\x20-\x7e]{4,}", data):
                    sys.stdout.buffer.write(value + b"\n")


def main():
    command, *args = sys.argv[1:]
    if command == "export":
        layout, output, arches, module, *binaries = args
        export(layout, output, arches.split(","), module, binaries)
    elif command == "compare":
        compare(*args)
    elif command == "scan":
        scan(args[0], args[1:])
    elif command == "strings":
        binary_strings(*args)
    else:
        raise ValueError("unknown content command")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, tarfile.TarError, EOFError):
        print("release content validation failed; inspect locally without exposing findings", file=sys.stderr)
        sys.exit(1)
