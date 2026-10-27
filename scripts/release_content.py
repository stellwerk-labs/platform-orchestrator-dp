#!/usr/bin/env python3
"""Inspect actual server OCI payloads without materializing untrusted links."""
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tempfile
import zlib


def decode_gzip_members(data, *, max_output=256 * 1024 * 1024,
                        max_metadata=1024 * 1024, max_members=1024):
    """Validate bounded RFC 1952 members and retain their non-payload bytes."""
    if min(max_output, max_metadata, max_members) < 1:
        raise ValueError("invalid gzip bounds")
    payloads, metadata, offset, output_size, metadata_size, members = [], [], 0, 0, 0, 0

    def retain(value):
        nonlocal metadata_size
        metadata_size += len(value)
        if metadata_size > max_metadata:
            raise ValueError("gzip metadata exceeds bound")
        metadata.append(value)

    while offset < len(data):
        padding_end = offset
        while padding_end < len(data) and data[padding_end] == 0:
            padding_end += 1
        if padding_end != offset:
            if not members:
                raise ValueError("gzip starts with padding")
            retain(data[offset:padding_end])
            offset = padding_end
        if offset == len(data):
            break
        members += 1
        if members > max_members:
            raise ValueError("gzip member count exceeds bound")
        if data[offset:offset + 3] != b"\x1f\x8b\x08" or offset + 10 > len(data):
            raise ValueError("invalid gzip member header or trailing bytes")
        flags, header_end = data[offset + 3], offset + 10
        if flags & 0xe0:
            raise ValueError("reserved gzip header flags")
        if flags & 4:
            if header_end + 2 > len(data):
                raise ValueError("truncated gzip extra length")
            size = int.from_bytes(data[header_end:header_end + 2], "little")
            header_end += 2 + size
        if header_end > len(data):
            raise ValueError("truncated gzip extra field")
        for flag in (8, 16):
            if flags & flag:
                end = data.find(b"\0", header_end,
                                min(len(data), offset + max_metadata - metadata_size + 1))
                if end < 0:
                    raise ValueError("unterminated or oversized gzip metadata")
                header_end = end + 1
        if flags & 2:
            header_end += 2
        if header_end > len(data):
            raise ValueError("truncated gzip header checksum")
        retain(data[offset:header_end])
        decoder = zlib.decompressobj(wbits=31)
        try:
            payload = decoder.decompress(memoryview(data)[offset:], max_output - output_size + 1)
        except zlib.error as error:
            raise ValueError("gzip member checksum or compression invalid") from error
        output_size += len(payload)
        if output_size > max_output:
            raise ValueError("gzip decompressed bytes exceed bound")
        if not decoder.eof or decoder.unconsumed_tail:
            raise ValueError("truncated or oversized gzip member")
        member_end = len(data) - len(decoder.unused_data)
        if member_end < header_end + 8:
            raise ValueError("invalid gzip member bounds")
        retain(data[member_end - 8:member_end])
        payloads.append(payload)
        offset = member_end
    if not members:
        raise ValueError("gzip has no members")
    return b"".join(payloads), b"".join(metadata)


def strict_json(data):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate OCI JSON key")
            result[key] = value
        return result

    return json.loads(data, object_pairs_hook=unique)


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


def oci_reference(directory, expected):
    layout = Path(directory)
    index_file = layout / "index.json"
    if index_file.is_symlink() or not index_file.is_file():
        raise ValueError("missing or symlinked OCI index")
    descriptors = strict_json(index_file.read_bytes()).get("manifests", [])
    if (len(descriptors) != 1 or descriptors[0].get("digest") != expected
            or descriptors[0].get("mediaType") != "application/vnd.oci.image.index.v1+json"):
        raise ValueError("OCI reference differs from inspected index")
    blob(layout, descriptors[0])
    tag = descriptors[0].get("annotations", {}).get("org.opencontainers.image.ref.name", "")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
        raise ValueError("missing or invalid verified OCI tag")
    return tag


def validate_sbom_layout(directory, expected):
    layout = Path(directory)
    index_file = layout / "index.json"
    if index_file.is_symlink() or not index_file.is_file():
        raise ValueError("missing or symlinked SBOM OCI index")
    descriptors = strict_json(index_file.read_bytes()).get("manifests", [])
    if (len(descriptors) != 1 or descriptors[0].get("digest") != expected
            or descriptors[0].get("mediaType") != "application/vnd.oci.image.manifest.v1+json"):
        raise ValueError("SBOM source differs from inspected platform manifest")
    manifest = strict_json(blob(layout, descriptors[0]))
    for descriptor in [manifest["config"]] + manifest["layers"]:
        blob(layout, descriptor)


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
    marker_bytes = (layout / "oci-layout").read_bytes()
    if strict_json(marker_bytes) != {"imageLayoutVersion": "1.0.0"}:
        raise ValueError("unsupported OCI layout")
    index_bytes = (layout / "index.json").read_bytes()
    index = strict_json(index_bytes)
    if len(index["manifests"]) != 1:
        raise ValueError("layout must contain one release index")
    top = index["manifests"][0]
    image_index_bytes = blob(layout, top)
    image_index = strict_json(image_index_bytes)
    manifests = image_index.get("manifests")
    if image_index.get("schemaVersion") != 2 or not isinstance(manifests, list):
        raise ValueError("release must be an OCI image index")
    payloads = {}
    attestations = []
    output.mkdir()
    metadata = output / "metadata"
    metadata.mkdir()
    # Retain original bytes as well as decoded values for strict scanning.
    (metadata / "oci-layout.json").write_bytes(marker_bytes)
    (metadata / "layout-index.json").write_bytes(index_bytes)
    (metadata / "index.json").write_bytes(image_index_bytes)
    (metadata / "layout-index-decoded.json").write_text(json.dumps(index, sort_keys=True))
    (metadata / "index-decoded.json").write_text(json.dumps(image_index, sort_keys=True))
    for descriptor in manifests:
        platform = descriptor.get("platform", {})
        manifest_bytes = blob(layout, descriptor)
        manifest = strict_json(manifest_bytes)
        config_bytes = blob(layout, manifest["config"])
        config = strict_json(config_bytes)
        identity = descriptor["digest"][7:]
        (metadata / (identity + "-manifest.json")).write_bytes(manifest_bytes)
        (metadata / (manifest["config"]["digest"][7:] + "-config.json")).write_bytes(config_bytes)
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
                statement = strict_json(data)
                if statement.get("_type") not in ("https://in-toto.io/Statement/v0.1", "https://in-toto.io/Statement/v1"):
                    raise ValueError("unsupported attestation statement")
                predicates.add(statement.get("predicateType"))
                if not any(subject.get("digest", {}).get("sha256") == reference.removeprefix("sha256:")
                           for subject in statement.get("subject", [])):
                    raise ValueError("attestation subject does not bind platform manifest")
                (metadata / (identity + "-" + str(number) + ".json")).write_bytes(data)
                (metadata / (identity + "-" + str(number) + "-decoded.json")).write_text(json.dumps(statement, sort_keys=True))
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
                raw, gzip_metadata = decode_gzip_members(data)
            elif media == "application/vnd.oci.image.layer.v1.tar":
                raw = data
                gzip_metadata = b""
            else:
                raise ValueError("unsupported image layer compression")
            diff_ids.append(digest(raw))
            layers.append(layer["digest"])
            raw_root = output / "layers" / arch / str(number)
            raw_root.mkdir(parents=True)
            (raw_root / "gzip-metadata.bin").write_bytes(gzip_metadata)
            entries, seen, metadata_chunks, cursor = [], set(), [], 0
            with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
                for entry_number, entry in enumerate(archive):
                    if entry.size < 0:
                        raise ValueError("negative layer member size")
                    if entry.isdir() and entry.name in (".", "./"):
                        continue
                    name = safe_name(entry.name)
                    if name in seen:
                        raise ValueError("duplicate layer entry")
                    seen.add(name)
                    record = {"name": name, "mode": entry.mode, "type": entry.type.decode("ascii"), "link": entry.linkname}
                    entries.append(record)
                    if entry.isfile():
                        start, end = entry.offset_data, entry.offset_data + entry.size
                        if start < cursor or end > len(raw):
                            raise ValueError("invalid layer content offsets")
                        # Keep every header/PAX/padding byte separate from the
                        # executable bodies that receive per-file inspection.
                        metadata_chunks.append(raw[cursor:start])
                        cursor = end
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
                        if len(content) != entry.size:
                            raise ValueError("layer member size mismatch")
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
            metadata_chunks.append(raw[cursor:])
            (raw_root / "metadata-bytes.bin").write_bytes(b"".join(metadata_chunks))
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
    # Distroless's conventional container home is not a developer identity.
    common = [rb"/(?:Users/[A-Za-z0-9_.-]+|home/(?!nonroot/)[A-Za-z0-9_.-]+)/", rb"github\.com/[^\s\"'<>]+-enterprise\b",
              rb"\bhttps?://[a-z0-9.-]+\.(?:internal|localdomain)\b"] + [re.escape(value.encode()) for value in forbidden if value]
    text = [rb"\bWX[0-9]+\b", rb"\b(?:S1\.[0-9]+|PM[0-9]{2}|MO[0-9]{2}|[QCD][0-9]{2})\b"]
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix().encode()
        if path.name == ".gitleaksignore":
            raise ValueError("artifact-provided scanner ignore files are prohibited")
        if any(re.search(pattern, relative) for pattern in common + text):
            raise ValueError("private reference scan found prohibited content")
        if path.is_symlink():
            raise ValueError("refusing symlinked scan content")
        if path.is_file():
            data = path.read_bytes()
            if any(re.search(pattern, data) for pattern in common + ([] if b"\0" in data else text)):
                raise ValueError("private reference scan found prohibited content")


KIN_MODULE = "github.com/getkin/kin-openapi"
KIN_VERSION = "v0.144.0"
KIN_SUM = "h1:hIRcTH+KjLfkLpYU6bSSfdFpi0fZi1fp+hSPi4aQu9Y="
BINARY_POLICY = "kin-openapi-v0.144.0-go1.26.10-adjacent-validation-v1"
KIN_MATCH = "41678ee534db27563e9f7717169f4f1b0d2dc051912e2da8f8c9c33e724364b1"
KIN_SECRET = "8b422fbcf44a15e60d9edc56c00c549209107dee95a0332e5cf560d4cbc961ab"
KIN_BEFORE = "c70bd16e531b368f911bab880f76007541bf617d3488ae15455b1a389ea524da"
KIN_AFTER = "feb9e59b60eab45bc01bb66a0e3f5c9a1e432b819c06e0324334cf1266bfcb32"


def known_binary_finding(data, finding, sums, buildinfo):
    """Accept only the reviewed compiler boundary, never a source credential."""
    if (data[:6] != b"\x7fELF\x02\x01" or len(data) < 20
            or int.from_bytes(data[18:20], "little") not in (62, 183)
            or finding.get("RuleID") != "generic-api-key"
            or not isinstance(finding.get("Match"), str)
            or not isinstance(finding.get("Secret"), str)):
        return False
    match = finding["Match"].encode()
    if len(match) != 26 or hashlib.sha256(match).hexdigest() != KIN_MATCH:
        return False
    secret = finding["Secret"].encode()
    if len(secret) != 15 or hashlib.sha256(secret).hexdigest() != KIN_SECRET or secret not in match:
        return False
    identity = [KIN_MODULE, KIN_VERSION, KIN_SUM]
    if identity not in [line.split() for line in sums.splitlines()]:
        return False
    rows = [line.split() for line in buildinfo.splitlines()]
    if not buildinfo.splitlines() or not buildinfo.splitlines()[0].endswith(": go1.26.10"):
        return False
    deps = [i for i, row in enumerate(rows) if len(row) > 1 and row[:2] == ["dep", KIN_MODULE]]
    if (len(deps) != 1 or rows[deps[0]] != ["dep"] + identity
            or (deps[0] + 1 < len(rows) and rows[deps[0] + 1][:1] == ["=>"])):
        return False
    # The finding crosses last13/first13 of two complete 21-byte literals.
    # Validate both adjacent source literals at the same binary occurrence.
    occurrences = list(re.finditer(re.escape(match), data))
    for occurrence in occurrences:
        boundary = occurrence.start() + 13
        before, after = data[max(0, boundary - 21):boundary], data[boundary:boundary + 21]
        if (len(before) != 21 or len(after) != 21
                or hashlib.sha256(before).hexdigest() != KIN_BEFORE
                or hashlib.sha256(after).hexdigest() != KIN_AFTER):
            return False
    return bool(occurrences)


def scanner_findings(returncode, stdout, stderr):
    findings = json.loads(stdout)
    if (returncode not in (0, 1) or stderr or not isinstance(findings, list)
            or (returncode == 0) != (len(findings) == 0)
            or any(not isinstance(finding, dict)
                   or any(not isinstance(finding.get(key), str) or not finding[key]
                          for key in ("RuleID", "Match", "Secret")) for finding in findings)):
        raise ValueError("binary scanner tool or report failure")
    return findings


NOTICE_POLICY = "hashicorp-public-source-notices-v1"
# go-licenses v2.0.1 preserves reciprocal-library source, including these
# upstream test fixtures. Bind the complete public files, not secret patterns.
NOTICE_MODULES = {
    "github.com/hashicorp/hcl": ("v1.0.1-vault-7", "h1:ag5OxFVy3QYTFTJODRzTKVZ6xvdfLLCA1cy/Y6xGI0I="),
    "github.com/hashicorp/vault/api": ("v1.23.0", "h1:gXgluBsSECfRWTSW9niY2jwg2e9mMJc4WoHNv4g3h6A="),
    "github.com/hashicorp/vault/api/auth/kubernetes": ("v0.10.0", "h1:5rqWmUFxnu3S7XYq9dafURwBgabYDFzo2Wv+AMopPHs="),
}
NOTICE_FILES = {
    "github.com/hashicorp/hcl/json/scanner/scanner_test.go":
        ("fe5824fbefc9283a92072a25d50ef5938ad3176e72ad48b90670d213771785fa", "generic-api-key",
         ((106, 106, 5, 37), (110, 110, 5, 36), (134, 134, 5, 36), (138, 138, 5, 37), (142, 142, 5, 38), (146, 146, 5, 37))),
    "github.com/hashicorp/hcl/hcl/scanner/scanner_test.go":
        ("8fd8ccada793173cdad28e260637e5187238b111ba97493b87de89c2c4113381", "generic-api-key",
         ((119, 119, 5, 37), (125, 125, 5, 37), (142, 142, 5, 38), (148, 148, 5, 38), (191, 191, 5, 37),
          (195, 195, 5, 36), (219, 219, 5, 36), (223, 223, 5, 37), (227, 227, 5, 38), (231, 231, 5, 37))),
    "github.com/hashicorp/vault/api/test-fixtures/keys/key.pem":
        ("f776bd22e8ac0a1ed2b76ddd9e3f4ea0cf5ad1e8d7165033b28532214ed9ce2a", "private-key", ((1, 27, 1, 30),)),
    "github.com/hashicorp/vault/api/test-fixtures/root/pkioutput":
        ("25311302e804833627b63368ab473785399ab3f9bf5f5ff6fe5a90f14ad10b62", "private-key", ((46, 72, 19, 30),)),
    "github.com/hashicorp/vault/api/test-fixtures/root/rootcakey.pem":
        ("9e32eacb685fc948ff353f7d66411e0e95a47f58705c7885258e7dd169f8bb8e", "private-key", ((1, 27, 1, 30),)),
    "github.com/hashicorp/vault/api/test-fixtures/keys/pkioutput":
        ("32a47442a9485f60c5c4e519d89c74b554c288d24cb4a6d96b8bf44a6eaf70fc", "private-key", ((47, 73, 19, 30),)),
    "github.com/hashicorp/vault/api/auth/kubernetes/go.sum":
        ("8d10769bd75929527a9b611c37c53fab08cec31305cd607ad0fb5461e6ef2c24", "generic-api-key", ((40, 40, 29, 87),)),
}


def notice_identity(root, path):
    """Resolve only exporter-created merged or raw-layer regular-file paths."""
    parts = path.relative_to(root).parts
    if len(parts) >= 4 and parts[0] in ("amd64", "arm64") and parts[1] == "licenses":
        return parts[0], "/".join(parts[2:])
    if (len(parts) == 4 and parts[0] == "layers" and parts[1] in ("amd64", "arm64")
            and parts[2].isdigit() and parts[3].isdigit()):
        entries = json.loads((path.parent / "entries.json").read_bytes())
        number = int(parts[3])
        if not isinstance(entries, list) or number >= len(entries):
            return None
        entry = entries[number]
        if not isinstance(entry, dict) or entry.get("type") != "0" or entry.get("link") != "":
            return None
        name = safe_name(entry["name"])
        if name.startswith("licenses/"):
            return parts[1], name[9:]
    return None


def known_notice_findings(root, path, findings, source):
    identity = notice_identity(root, path)
    if identity is None:
        return False
    arch, name = identity
    if name not in NOTICE_FILES:
        return False
    expected_hash, rule, positions = NOTICE_FILES[name]
    if hashlib.sha256(path.read_bytes()).hexdigest() != expected_hash:
        return False
    module = max((module for module in NOTICE_MODULES if name.startswith(module + "/")), key=len)
    version, checksum = NOTICE_MODULES[module]
    sums = [line.split() for line in (source / "go.sum").read_text().splitlines()]
    if [module, version, checksum] not in sums:
        return False
    mod = (source / "go.mod").read_text()
    if (not re.search(r"(?m)^\s*(?:require\s+)?" + re.escape(module + " " + version) + r"(?:\s*//[^\n]*)?\s*$", mod)
            or re.search(r"(?m)^\s*(?:replace\s+)?" + re.escape(module) + r"(?:\s+[^\s]+)?\s*=>", mod)):
        return False
    graph = (root / arch / "licenses/modules.txt").read_text().splitlines()
    if [line for line in graph if line.startswith(module + "|")] != [module + "|" + version]:
        return False
    actual = []
    for finding in findings:
        keys = ("StartLine", "EndLine", "StartColumn", "EndColumn")
        if finding.get("RuleID") != rule or any(type(finding.get(key)) is not int for key in keys):
            return False
        actual.append(tuple(finding[key] for key in keys))
    return sorted(actual) == sorted(positions)


def source_scan(directory):
    """Keep Gitleaks reports redacted and apply only exact public-file policy."""
    root = Path(directory).resolve()
    source = Path(__file__).resolve().parents[1]
    result = subprocess.run(["gitleaks", "dir", "--redact=100", "--no-banner", "--log-level", "error",
                             "--ignore-gitleaks-allow", "--gitleaks-ignore-path", "/dev/null",
                             "--config", str(source / "scripts/gitleaks.toml"),
                             "--report-format", "json", "--report-path", "-", "."],
                            capture_output=True, timeout=120, cwd=root)
    findings = scanner_findings(result.returncode, result.stdout, result.stderr)
    groups = {}
    for finding in findings:
        name = finding.get("File")
        if not isinstance(name, str) or name != safe_name(name):
            raise ValueError("source scanner report path invalid")
        path = root / name
        if not path.is_file() or path.is_symlink():
            raise ValueError("source scanner finding is not a regular file")
        groups.setdefault(path, []).append(finding)
    for path, group in groups.items():
        if not known_notice_findings(root, path, group, source):
            raise ValueError("source secret scan found unapproved content")
    print(json.dumps({"acceptedPublicSourceFindings": len(findings), "policy": NOTICE_POLICY}, sort_keys=True))


def binary_scan(directory):
    root = Path(directory)
    if not root.is_dir() or root.is_symlink():
        raise ValueError("missing or symlinked scan input")
    source = Path(__file__).resolve().parents[1]
    sums = (source / "go.sum").read_text()
    accepted = 0
    seen = set()
    other_strings = []
    names = []
    def inspect(strings):
        with tempfile.TemporaryDirectory(prefix="binary-scan-") as isolated:
            result = subprocess.run(["gitleaks", "stdin", "--no-banner", "--log-level", "error",
                                     "--ignore-gitleaks-allow", "--gitleaks-ignore-path", "/dev/null",
                                     "--config", str(source / "scripts/gitleaks.toml"),
                                     "--report-format", "json", "--report-path", "-"],
                                    input=strings, capture_output=True, timeout=65, cwd=isolated)
        return scanner_findings(result.returncode, result.stdout, result.stderr)
    for path in root.rglob("*"):
        names.append(path.relative_to(root).as_posix().encode())
        if path.is_symlink():
            raise ValueError("refusing symlinked scan content")
        if not path.is_file():
            continue
        data = path.read_bytes()
        if b"\0" not in data:
            continue
        identity = hashlib.sha256(data).digest()
        if identity in seen:
            continue
        seen.add(identity)
        strings = b"\n".join(re.findall(rb"[\x20-\x7e]{4,}", data)) + b"\n"
        if data[:6] != b"\x7fELF\x02\x01":
            other_strings.append(strings)
            continue
        # Raw finding values exist only in this process's memory. Neither tool
        # output nor reports are logged or persisted, including on failure.
        findings = inspect(strings)
        if not findings:
            continue
        environment = dict(os.environ, GOTOOLCHAIN="go1.26.10", GOENV="off", GOWORK="off")
        metadata = subprocess.run(["go", "version", "-m", str(path)], capture_output=True,
                                  env=environment, timeout=15)
        if metadata.returncode or metadata.stderr:
            raise ValueError("binary finding lacks verified Go build metadata")
        buildinfo = metadata.stdout.decode()
        for finding in findings:
            if not known_binary_finding(data, finding, sums, buildinfo):
                raise ValueError("binary secret scan found unapproved content")
            accepted += 1
    # Binary data such as timezone databases has no exception. Batch its strings
    # rather than spawning one scanner process per tiny file. Byte-identical
    # raw/merged files were already checked once, including hidden lower layers.
    if other_strings and inspect(b"\n".join(other_strings)):
        raise ValueError("non-ELF binary secret scan found prohibited content")
    if inspect(b"\n".join(names)):
        raise ValueError("artifact path secret scan found prohibited content")
    print(json.dumps({"acceptedBinaryFalsePositives": accepted, "policy": BINARY_POLICY}, sort_keys=True))


def main():
    command, *args = sys.argv[1:]
    if command == "export":
        layout, output, arches, module, *binaries = args
        export(layout, output, arches.split(","), module, binaries)
    elif command == "compare":
        compare(*args)
    elif command == "scan":
        scan(args[0], args[1:])
    elif command == "oci-reference":
        print(oci_reference(*args))
    elif command == "sbom-layout":
        validate_sbom_layout(*args)
    elif command == "binary-scan":
        binary_scan(*args)
    elif command == "source-scan":
        source_scan(*args)
    else:
        raise ValueError("unknown content command")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, tarfile.TarError, EOFError, subprocess.SubprocessError):
        print("release content validation failed; inspect locally without exposing findings", file=sys.stderr)
        sys.exit(1)
