#!/usr/bin/env python3
"""Real filesystem/OCI fixtures and mandatory pinned scanner checks."""
import contextlib
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import random
import re
import shutil
import string
import subprocess
import tarfile
import tempfile
import unittest
import zlib

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("release_content", ROOT / "scripts/release_content.py")
content = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(content)
MODULE = "example.org/application"


def encoded(value):
    return json.dumps(value, sort_keys=True).encode()


def layer(entries, pax_metadata=None, trailing_bytes=b""):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as archive:
        for name, data in entries.items():
            entry = tarfile.TarInfo(name)
            entry.pax_headers = pax_metadata or {}
            entry.mode = 0o755 if name.startswith("opt/server/") else 0o644
            if isinstance(data, tuple):
                entry.type, entry.linkname = data
            else:
                entry.size = len(data)
            archive.addfile(entry, None if isinstance(data, tuple) else io.BytesIO(data))
    return raw.getvalue() + trailing_bytes


def gzip_member(raw, name=b"", comment=b"", extra=b"", header_crc=False):
    flags = (8 if name else 0) | (16 if comment else 0) | (4 if extra else 0) | (2 if header_crc else 0)
    header = b"\x1f\x8b\x08" + bytes([flags]) + b"\0" * 6
    if extra:
        header += len(extra).to_bytes(2, "little") + extra
    if name:
        header += name + b"\0"
    if comment:
        header += comment + b"\0"
    if header_crc:
        header += (zlib.crc32(header) & 0xffff).to_bytes(2, "little")
    compressor = zlib.compressobj(wbits=-15)
    return (header + compressor.compress(raw) + compressor.flush()
            + zlib.crc32(raw).to_bytes(4, "little") + (len(raw) & 0xffffffff).to_bytes(4, "little"))


def elf(arch):
    data = bytearray(b"\x7fELF\x02\x01" + b"\0" * 60)
    data[18:20] = (62 if arch == "amd64" else 183).to_bytes(2, "little")
    return bytes(data)


def license_bundle(arch, own_module=MODULE):
    files = {"application/LICENSE": b"application license", "go/LICENSE": b"Go license", "go/PATENTS": b"Go patent grant",
             "example.org/dependency/LICENSE": b"dependency license",
             "modules.txt": ("\n".join(sorted((own_module + "|", "example.org/dependency|v1.0.0"))) + "\n").encode(),
             "target.txt": ("linux/" + arch + "\n").encode()}
    files["manifest.sha256"] = "".join(hashlib.sha256(data).hexdigest() + "  ./" + name + "\n"
                                            for name, data in sorted(files.items())).encode()
    return {"licenses/" + name: data for name, data in files.items()}


def layout(root, extra=None, whiteout=False, mutate=None, binaries=("opt/server/server",), missing_provenance=False,
           pax_metadata=None, trailing_bytes=b"", compress=None, json_transform=None, own_module=MODULE):
    root.mkdir()
    def metadata(kind, data):
        return json_transform(kind, data) if json_transform else data
    (root / "oci-layout").write_bytes(metadata("marker", encoded({"imageLayoutVersion": "1.0.0"})))
    blobroot = root / "blobs/sha256"
    blobroot.mkdir(parents=True)
    def store(data, media):
        kinds = {"application/vnd.oci.image.config.v1+json": "config",
                 "application/vnd.oci.image.manifest.v1+json": "manifest",
                 "application/vnd.oci.image.index.v1+json": "index",
                 "application/vnd.in-toto+json": "statement"}
        if media in kinds:
            data = metadata(kinds[media], data)
        descriptor = {"mediaType": media, "digest": content.digest(data), "size": len(data)}
        (blobroot / descriptor["digest"][7:]).write_bytes(data)
        return descriptor
    manifests = []
    for arch in ("amd64", "arm64"):
        entries = license_bundle(arch, own_module)
        entries.update({name: elf(arch) for name in binaries})
        if extra:
            entries.update(extra)
        raws = [layer(entries, pax_metadata, trailing_bytes)]
        if whiteout:
            raws.append(layer({"opt/server/.wh.removed": b""}))
        config = {"os": "linux", "architecture": arch, "config": {"User": "65532:65532", "Entrypoint": ["/opt/server/server"]},
                  "rootfs": {"type": "layers", "diff_ids": [content.digest(raw) for raw in raws]}}
        image = {"schemaVersion": 2, "config": store(encoded(config), "application/vnd.oci.image.config.v1+json"),
                 "layers": [store(compress(raw) if compress else gzip.compress(raw, mtime=0),
                                  "application/vnd.oci.image.layer.v1.tar+gzip") for raw in raws]}
        if mutate:
            mutate(arch, image, config, store)
        descriptor = store(encoded(image), "application/vnd.oci.image.manifest.v1+json")
        descriptor["platform"] = {"os": "linux", "architecture": arch}
        manifests.append(descriptor)
        statements = [{"_type": "https://in-toto.io/Statement/v0.1", "subject": [{"digest": {"sha256": descriptor["digest"][7:]}}],
                       "predicateType": kind, "predicate": {}} for kind in ("https://spdx.dev/Document", "https://slsa.dev/provenance/v0.2")]
        if missing_provenance:
            statements.pop()
        attestation = {"schemaVersion": 2, "config": store(b'{"fixture":true}', "application/vnd.oci.image.config.v1+json"),
                       "layers": [store(encoded(statement), "application/vnd.in-toto+json") for statement in statements]}
        attestation_descriptor = store(encoded(attestation), "application/vnd.oci.image.manifest.v1+json")
        attestation_descriptor.update(platform={"os": "unknown", "architecture": "unknown"},
                                      annotations={"vnd.docker.reference.type": "attestation-manifest",
                                                   "vnd.docker.reference.digest": descriptor["digest"]})
        manifests.append(attestation_descriptor)
    descriptor = store(encoded({"schemaVersion": 2, "manifests": manifests}), "application/vnd.oci.image.index.v1+json")
    descriptor["annotations"] = {"org.opencontainers.image.ref.name": "qualification"}
    (root / "index.json").write_bytes(metadata("wrapper", encoded({"schemaVersion": 2, "manifests": [descriptor]})))


class ContentTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not shutil.which("gitleaks"):
            raise RuntimeError("pinned gitleaks is required, scanner checks must not skip")
        subprocess.run(["gitleaks", "version"], check=True, capture_output=True)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def export(self, **options):
        image, output = self.root / "image", self.root / "output"
        layout(image, **options)
        return content.export(image, output, ["amd64", "arm64"], MODULE,
                              list(options.get("binaries", ("opt/server/server",))))

    def scanner(self, directory, source=ROOT):
        return subprocess.run(["bash", str(source / "scripts/scan-release-content.sh"), str(directory)],
                              text=True, capture_output=True, timeout=30)

    def test_original_metadata_bytes_and_decoded_references_are_scanned(self):
        for kind in ("wrapper", "index", "manifest", "config", "statement"):
            for value in (b"private-owner-fixture", b"private-owner-fixtur\\u0065"):
                with self.subTest(kind=kind, escaped=b"\\u" in value), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    originals = []
                    def transform(stage, data):
                        if stage == kind:
                            data = b'{ "audit":"' + value + b'",\n' + data[1:]
                            originals.append(data)
                        return data
                    layout(root / "image", json_transform=transform)
                    content.export(root / "image", root / "output", ["amd64", "arm64"], MODULE, ["opt/server/server"])
                    retained = [path.read_bytes() for path in (root / "output/metadata").iterdir()]
                    self.assertTrue(originals)
                    for raw in originals:
                        self.assertIn(raw, retained)
                    with self.assertRaisesRegex(ValueError, "prohibited content"):
                        content.scan(root / "output", ["private-owner-fixture"])

    def artifact_check(self, image, output):
        return subprocess.run(["bash", str(ROOT / "scripts/check-server-artifact.sh"), str(image), str(output)],
                              capture_output=True, text=True, timeout=30)

    def publication_layout(self, path, transform=None):
        module = (ROOT / "go.mod").read_text().splitlines()[0].removeprefix("module ")
        binaries = tuple(re.findall(r"opt/server/[a-z-]+", (ROOT / "scripts/check-server-artifact.sh").read_text()))
        layout(path, own_module=module, binaries=binaries, json_transform=transform)

    def test_duplicate_metadata_keys_fail_at_both_inspection_boundaries(self):
        for kind in ("marker", "wrapper", "index", "manifest", "config", "statement"):
            for prefix in (b'{"audit":"private-owner-fixture","audit":"public",',
                           b'{"nested":{"audit":"private-owner-fixture","aud\\u0069t":"public"},'):
                with self.subTest(kind=kind, nested=b"nested" in prefix), tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    def transform(stage, data):
                        return prefix + data[1:] if stage == kind else data
                    self.publication_layout(root / "image", transform)
                    module = (ROOT / "go.mod").read_text().splitlines()[0].removeprefix("module ")
                    with self.assertRaisesRegex(ValueError, "duplicate OCI JSON key"):
                        content.export(root / "image", root / "direct", ["amd64", "arm64"], module, ["opt/server/server"])
                    for boundary in ("producer", "publisher"):
                        result = self.artifact_check(root / "image", root / boundary)
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn("release content validation failed", result.stderr)
                        self.assertNotIn("private-owner-fixture", result.stdout + result.stderr)

    def test_publication_reinspection_rejects_digest_valid_metadata_tamper(self):
        token = "ghp_" + "".join(random.Random(61).sample(string.ascii_letters + string.digits, 36))
        for kind in ("index", "manifest", "config"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                image = root / "image"
                self.publication_layout(image)
                producer = self.artifact_check(image, root / "producer")
                self.assertEqual(producer.returncode, 0, producer.stderr)
                publisher = self.artifact_check(image, root / "publisher-clean")
                self.assertEqual(publisher.returncode, 0, publisher.stderr)
                self.assertEqual((root / "producer/report.json").read_bytes(),
                                 (root / "publisher-clean/report.json").read_bytes())
                shutil.rmtree(image)
                def transform(stage, data):
                    return b'{"audit":"' + token.encode() + b' # gitleaks:allow",' + data[1:] if stage == kind else data
                self.publication_layout(image, transform)
                for boundary in ("producer-tampered", "publisher-tampered"):
                    tampered = self.artifact_check(image, root / boundary)
                    self.assertNotEqual(tampered.returncode, 0)
                    self.assertNotIn(token, tampered.stdout + tampered.stderr)

    def test_platform_bytes_notices_and_attestations(self):
        report = self.export(extra={"etc/link": (tarfile.SYMTYPE, "/etc/certs"), "etc/certs": b"base certificate"})
        self.assertEqual(set(report["payloads"]), {"amd64", "arm64"})
        self.assertEqual(len(report["attestations"]), 2)
        self.assertEqual((self.root / "output/arm64/opt/server/server").read_bytes(), elf("arm64"))
        self.assertFalse((self.root / "output/amd64/etc/link").exists())
        self.assertEqual(self.scanner(self.root / "output").returncode, 0)

    def test_gzip_optional_metadata_in_every_member_is_scanned(self):
        seed = ("ghp_" + "".join(random.Random(41).sample(string.ascii_letters + string.digits, 36))).encode()
        for field in ("name", "comment", "extra"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                def compress(raw):
                    return gzip_member(raw[:512], name=b"public-base-layer", header_crc=True) + gzip_member(
                        raw[512:], header_crc=True, **{field: seed + b" # gitleaks:allow"})
                layout(root / "image", compress=compress)
                content.export(root / "image", root / "output", ["amd64", "arm64"], MODULE, ["opt/server/server"])
                result = self.scanner(root / "output")
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn(seed.decode(), result.stdout + result.stderr)

    def test_gzip_member_validation_and_bounds(self):
        first = gzip_member(b"first", name=b"base", comment=b"public", extra=b"metadata", header_crc=True)
        second = gzip_member(b"second")
        raw, metadata = content.decode_gzip_members(first + b"\0\0" + second + b"\0")
        self.assertEqual(raw, b"firstsecond")
        self.assertIn(b"base\0public\0", metadata)
        self.assertIn(b"metadata", metadata)
        self.assertTrue(metadata.endswith(b"\0"))
        for malformed in (b"", b"\0" + first, first[:-1], first + b"not-gzip", first[:3] + b"\xe0" + first[4:],
                          first[:-8] + bytes([first[-8] ^ 1]) + first[-7:],
                          first[:-4] + bytes([first[-4] ^ 1]) + first[-3:],
                          first[:10] + b"\xff\xff" + first[12:],
                          gzip_member(b"data", name=b"name")[:12]):
            with self.subTest(malformed=hashlib.sha256(malformed).hexdigest()), self.assertRaises(ValueError):
                content.decode_gzip_members(malformed)
        for options in ({"max_output": 4}, {"max_metadata": 8}, {"max_members": 1}, {"max_members": 0}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                content.decode_gzip_members(first + second, **options)

    def test_conventional_container_home_is_not_a_private_home_or_secret_exception(self):
        target = self.root / "notice"
        target.write_bytes(b"public container home /home/nonroot/\0")
        self.assertEqual(self.scanner(self.root).returncode, 0)
        for private in (b"/home/jay/", b"/Users/nonroot/", b"/home/nonroot-other/"):
            target.write_bytes(private + b"\0")
            with self.subTest(private=private), self.assertRaises(ValueError):
                content.scan(self.root)
        seed = "ghp_" + "".join(random.Random(43).sample(string.ascii_letters + string.digits, 36))
        target.write_text("/home/nonroot/" + seed)
        result = self.scanner(self.root)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(seed, result.stdout + result.stderr)

    def test_each_shipped_binary_is_required(self):
        self.export(binaries=("opt/server/server", "opt/server/authorization-migrate"))

    def test_traversal_fails(self):
        with self.assertRaises(ValueError):
            self.export(extra={"../escape": b"no"})

    def test_required_payload_link_ancestor_fails(self):
        with self.assertRaises(ValueError):
            self.export(extra={"opt": (tarfile.SYMTYPE, "/other")})

    def test_missing_buildkit_provenance_fails(self):
        with self.assertRaises(ValueError):
            self.export(missing_provenance=True)

    def test_corrupt_blob_fails(self):
        image = self.root / "image"
        layout(image)
        next((image / "blobs/sha256").iterdir()).write_bytes(b"corrupt")
        with self.assertRaises(ValueError):
            content.export(image, self.root / "output", ["amd64", "arm64"], MODULE, ["opt/server/server"])

    def test_license_hash_target_and_dependency_required(self):
        files = license_bundle("amd64")
        self.assertTrue(content.notices(files, "amd64", MODULE))
        for name in ("licenses/go/PATENTS", "licenses/example.org/dependency/LICENSE"):
            broken = dict(files)
            del broken[name]
            with self.assertRaises(ValueError):
                content.notices(broken, "amd64", MODULE)
        with self.assertRaises(ValueError):
            content.notices(files, "arm64", MODULE)
        files["licenses/go/LICENSE"] += b"changed"
        with self.assertRaises(ValueError):
            content.notices(files, "amd64", MODULE)

    def test_architecture_mismatch_fails(self):
        with self.assertRaises(ValueError):
            self.export(extra={"opt/server/server": elf("arm64")})

    def test_platform_compare_ignores_only_envelope_variance(self):
        report = self.export()
        other = dict(report, index="sha256:" + "f" * 64, attestations=[])
        first, second = self.root / "first.json", self.root / "second.json"
        first.write_bytes(encoded(report))
        second.write_bytes(encoded(other))
        with contextlib.redirect_stdout(io.StringIO()) as output:
            content.compare(first, second)
        self.assertIn('"fullIndexIdentical": false', output.getvalue())
        other["payloads"]["amd64"]["layers"] = []
        second.write_bytes(encoded(other))
        with self.assertRaises(ValueError):
            content.compare(first, second)

    def test_real_binary_and_whiteout_secret_scans(self):
        token = "ghp_" + "".join(random.Random(17).sample(string.ascii_letters + string.digits, 36))
        self.export(extra={"opt/server/removed": b"\x7fELF\0" + token.encode() + b"\0"}, whiteout=True)
        result = self.scanner(self.root / "output")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(token, result.stdout + result.stderr)

    def test_pax_headers_and_unparsed_trailing_bytes_are_scanned(self):
        token = "ghp_" + "".join(random.Random(29).sample(string.ascii_letters + string.digits, 36))
        for name, options in (
                ("pax", dict(pax_metadata={"public.metadata": token + " # gitleaks:allow"})),
                ("trailing", dict(trailing_bytes=b"\0" + token.encode() + b"\0"))):
            with self.subTest(metadata=name):
                image, output = self.root / (name + "-image"), self.root / (name + "-output")
                layout(image, **options)
                content.export(image, output, ["amd64", "arm64"], MODULE, ["opt/server/server"])
                result = self.scanner(output)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn(token, result.stdout + result.stderr)

    def kin_source_pair(self):
        environment = dict(os.environ, GOTOOLCHAIN="go1.26.10", GOWORK="off", GOENV="off")
        metadata = json.loads(subprocess.check_output(
            ["go", "mod", "download", "-json", content.KIN_MODULE + "@" + content.KIN_VERSION],
            env=environment, text=True, timeout=30))
        self.assertEqual(metadata["Sum"], content.KIN_SUM)
        directory = Path(metadata["Dir"])
        goroot = Path(subprocess.check_output(["go", "env", "GOROOT"], env=environment, text=True, timeout=15).strip())
        parts = []
        for path, line, file_hash, literal_hash in (
                (directory / "openapi3/validation_error.go", 1518, "420050b8a5b9f92e44cfe2df1d46d2b1ebda5144a05308142ff9542369cb3ea0", content.KIN_BEFORE),
                (goroot / "src/text/template/option.go", 71, "fae2206d9c10cf31439ba9d616a2f8ead93f8e9b8fc12060fbf9cfbc842c0a43", content.KIN_AFTER)):
            source = path.read_bytes()
            self.assertEqual(hashlib.sha256(source).hexdigest(), file_hash)
            literals = re.findall(r'"(?:[^"\\]|\\.)*"', source.decode().splitlines()[line - 1])
            values = [json.loads(literal).encode() for literal in literals]
            matches = [value for value in values if hashlib.sha256(value).hexdigest() == literal_hash]
            self.assertEqual(len(matches), 1)
            self.assertEqual(len(matches[0]), 21)
            parts.append(matches[0])
        return b"".join(parts)

    def test_exact_binary_exception_boundaries(self):
        pair = self.kin_source_pair()
        match = pair[8:34]
        self.assertEqual(hashlib.sha256(match).hexdigest(), content.KIN_MATCH)
        data = elf("arm64") + pair + b"\0"
        finding = {"RuleID": "generic-api-key", "Match": match.decode(), "Secret": match[10:25].decode()}
        sums = " ".join((content.KIN_MODULE, content.KIN_VERSION, content.KIN_SUM)) + "\n"
        buildinfo = "fixture: go1.26.10\n\tdep\t" + "\t".join((content.KIN_MODULE, content.KIN_VERSION, content.KIN_SUM)) + "\n"
        self.assertTrue(content.known_binary_finding(data, finding, sums, buildinfo))
        changed_context = bytearray(data)
        changed_context[len(elf("arm64"))] ^= 1
        negatives = (
            (bytes(changed_context), finding, sums, buildinfo),
            (data, dict(finding, Match=finding["Match"] + "x"), sums, buildinfo),
            (data, dict(finding, Secret="changed"), sums, buildinfo),
            (data, dict(finding, RuleID="github-pat"), sums, buildinfo),
            (data, finding, sums.replace(content.KIN_VERSION, "v0.144.1"), buildinfo),
            (data, finding, sums.replace(content.KIN_SUM, "h1:changed="), buildinfo),
            (data, finding, sums, buildinfo.replace(content.KIN_VERSION, "v0.144.1")),
            (data, finding, sums, buildinfo.replace(content.KIN_SUM, "h1:changed=")),
            (data, finding, sums, buildinfo.replace("go1.26.10", "go1.26.3")),
            (data, finding, sums, buildinfo + "\t=>\t./replacement\n"),
            (b"not-ELF\0" + pair, finding, sums, buildinfo),
            (data + b"\0" + match, finding, sums, buildinfo),
        )
        for case in negatives:
            with self.subTest(case_hash=hashlib.sha256(case[0]).hexdigest()):
                self.assertFalse(content.known_binary_finding(*case))

    def test_real_go_elf_exception_and_additional_secret(self):
        pair = self.kin_source_pair()
        application = self.root / "elf-application"
        application.mkdir()
        (application / "go.mod").write_text("module example.org/elf-fixture\n\ngo 1.26.10\n\nrequire "
                                           + content.KIN_MODULE + " " + content.KIN_VERSION + "\n")
        (application / "go.sum").write_bytes((ROOT / "go.sum").read_bytes())
        (application / "main.go").write_text('package main\nimport ("fmt"; _ "github.com/getkin/kin-openapi/openapi3")\n'
                                            + "func main() { fmt.Print(" + json.dumps(pair.decode()) + ") }\n")
        directory = self.root / "real-elf-scan"
        directory.mkdir()
        executable = directory / "server"
        environment = dict(os.environ, GOTOOLCHAIN="go1.26.10", GOWORK="off", GOENV="off",
                           GOOS="linux", GOARCH="arm64", CGO_ENABLED="0", GOMAXPROCS="2")
        subprocess.run(["go", "build", "-p=2", "-mod=mod", "-trimpath", "-o", str(executable), "."],
                       cwd=application, env=environment, check=True, capture_output=True, timeout=120)
        # The fixture is a separate real source graph, not the component's
        # dependency graph. Bind the copied scanner to its actual Go lockfile.
        (application / "scripts").mkdir()
        for name in ("scan-release-content.sh", "release_content.py", "release-tools.sh", "gitleaks.toml"):
            shutil.copyfile(ROOT / "scripts" / name, application / "scripts" / name)
        result = self.scanner(directory, application)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"acceptedBinaryFalsePositives": 1', result.stdout)
        original = executable.read_bytes()
        token = "ghp_" + "".join(random.Random(19).sample(string.ascii_letters + string.digits, 36))
        executable.write_bytes(original + b"\0" + token.encode() + b"\0")
        result = self.scanner(directory, application)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(token, result.stdout + result.stderr)
        for before, after in ((content.KIN_VERSION.encode(), b"v0.144.1"),
                              (content.KIN_SUM.encode(), content.KIN_SUM.replace("hIR", "HIR").encode())):
            self.assertIn(before, original)
            executable.write_bytes(original.replace(before, after))
            self.assertNotEqual(self.scanner(directory, application).returncode, 0)

    def test_binary_scanner_report_failures_are_closed(self):
        self.assertEqual(content.scanner_findings(0, b"[]", b""), [])
        for status, output, errors in ((0, b"invalid-json", b""), (0, b"{}", b""),
                                       (0, b"[{}]", b""), (1, b"[]", b""),
                                       (2, b"[]", b""), (0, b"[]", b"tool error"),
                                       (1, b"[null]", b""), (1, b"[{}]", b"")):
            with self.subTest(status=status, report_hash=hashlib.sha256(output).hexdigest()):
                with self.assertRaises(ValueError):
                    content.scanner_findings(status, output, errors)

    def notice_source_fixture(self):
        directory = self.root / "public-source"
        source = self.root / "scanner-source"
        (source / "scripts").mkdir(parents=True)
        for name in ("scan-release-content.sh", "release_content.py", "release-tools.sh", "gitleaks.toml"):
            shutil.copyfile(ROOT / "scripts" / name, source / "scripts" / name)
        for name in ("go.mod", "go.sum"):
            shutil.copyfile(ROOT / name, source / name)
        modules = {}
        environment = dict(os.environ, GOTOOLCHAIN="go1.26.10", GOWORK="off", GOENV="off")
        for module, (version, checksum) in content.NOTICE_MODULES.items():
            metadata = json.loads(subprocess.check_output(
                ["go", "mod", "download", "-json", module + "@" + version], env=environment, timeout=30))
            self.assertEqual(metadata["Sum"], checksum)
            modules[module] = Path(metadata["Dir"])
        graph = "".join(module + "|" + version + "\n"
                        for module, (version, _) in sorted(content.NOTICE_MODULES.items()))
        merged = directory / "arm64/licenses"
        merged.mkdir(parents=True)
        (merged / "modules.txt").write_text(graph)
        raw = directory / "layers/arm64/0"
        raw.mkdir(parents=True)
        entries = []
        for number, (name, (expected, _, _)) in enumerate(content.NOTICE_FILES.items()):
            module = max((module for module in modules if name.startswith(module + "/")), key=len)
            data = (modules[module] / name[len(module) + 1:]).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), expected)
            path = merged / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            (raw / str(number)).write_bytes(data)
            entries.append({"name": "licenses/" + name, "type": "0", "link": "", "mode": 420})
        (raw / "entries.json").write_bytes(encoded(entries))
        return directory, source

    def test_exact_public_source_files_and_mutation_boundaries(self):
        directory, source = self.notice_source_fixture()
        result = self.scanner(directory, source)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"acceptedPublicSourceFindings": 41', result.stdout)
        name = next(iter(content.NOTICE_FILES))
        path = directory / "arm64/licenses" / name
        original = path.read_bytes()
        token = "ghp_" + "".join(random.Random(53).sample(string.ascii_letters + string.digits, 36))
        # Complete source bytes, not merely known matching substrings, are bound.
        for addition in (b"\n// harmless mutation\n", b"\n" + token.encode() + b"\n"):
            path.write_bytes(original + addition)
            result = self.scanner(directory, source)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(token, result.stdout + result.stderr)
        path.write_bytes(original)
        relocated = directory / "relocated-source"
        path.rename(relocated)
        self.assertNotEqual(self.scanner(directory, source).returncode, 0)
        relocated.rename(path)
        graph = directory / "arm64/licenses/modules.txt"
        original_graph = graph.read_text()
        graph.write_text(original_graph.replace("v1.0.1-vault-7", "v1.0.1-vault-8"))
        self.assertNotEqual(self.scanner(directory, source).returncode, 0)
        graph.write_text(original_graph)
        for filename, before, after in (
                ("go.mod", "v1.0.1-vault-7", "v1.0.1-vault-8"),
                ("go.sum", content.NOTICE_MODULES["github.com/hashicorp/hcl"][1], "h1:changed=")):
            lockfile = source / filename
            original_lockfile = lockfile.read_text()
            lockfile.write_text(original_lockfile.replace(before, after))
            self.assertNotEqual(self.scanner(directory, source).returncode, 0)
            lockfile.write_text(original_lockfile)
        mod = source / "go.mod"
        original_mod = mod.read_text()
        mod.write_text(original_mod + "\nreplace github.com/hashicorp/hcl => ./replacement\n")
        self.assertNotEqual(self.scanner(directory, source).returncode, 0)
        mod.write_text(original_mod)
        raw_entries = directory / "layers/arm64/0/entries.json"
        original_entries = raw_entries.read_bytes()
        entries = json.loads(original_entries)
        entries[0]["name"] = "relocated/" + name
        raw_entries.write_bytes(encoded(entries))
        self.assertNotEqual(self.scanner(directory, source).returncode, 0)
        raw_entries.write_bytes(original_entries)
        # Unrelated content and raw metadata never receive a source-file exception.
        for relative, data in (("unrelated", token.encode()),
                               ("layers/arm64/0/metadata-bytes.bin", b"\0" + token.encode() + b"\0")):
            extra = directory / relative
            extra.write_bytes(data)
            result = self.scanner(directory, source)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(token, result.stdout + result.stderr)
            extra.unlink()

    def test_public_source_requires_exact_rules_locations_and_counts(self):
        directory, source = self.notice_source_fixture()
        name, (_, rule, positions) = next(iter(content.NOTICE_FILES.items()))
        path = directory / "arm64/licenses" / name
        keys = ("StartLine", "EndLine", "StartColumn", "EndColumn")
        findings = [dict(zip(keys, position), RuleID=rule) for position in positions]
        self.assertTrue(content.known_notice_findings(directory, path, findings, source))
        for changed in (findings[:-1], findings + [findings[0]],
                        [dict(findings[0], RuleID="private-key")] + findings[1:],
                        [dict(findings[0], StartLine=1)] + findings[1:]):
            self.assertFalse(content.known_notice_findings(directory, path, changed, source))

    def test_artifact_inline_allow_directives_cannot_suppress_secrets(self):
        token = "ghp_" + "".join(random.Random(31).sample(string.ascii_letters + string.digits, 36))
        for binary in (False, True):
            directory = self.root / ("binary-directive" if binary else "text-directive")
            directory.mkdir()
            (directory / "content").write_bytes((b"\0" if binary else b"") + token.encode()
                                                + b" # gitleaks:allow\n")
            result = self.scanner(directory)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(token, result.stdout + result.stderr)

    def test_artifact_ignore_file_cannot_suppress_secrets(self):
        directory = self.root / "ignore-file"
        directory.mkdir()
        token = "ghp_" + "".join(random.Random(37).sample(string.ascii_letters + string.digits, 36))
        (directory / "secret.txt").write_text(token + "\n")
        command = ["gitleaks", "dir", "--redact", "--no-banner", "--log-level", "error",
                   "--config", str(ROOT / "scripts/gitleaks.toml"), "--report-format", "json",
                   "--report-path", "-", "."]
        result = subprocess.run(command, cwd=directory, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        findings = json.loads(result.stdout)
        self.assertTrue(findings)
        (directory / ".gitleaksignore").write_text("\n".join(finding["Fingerprint"] for finding in findings) + "\n")
        self.assertEqual(subprocess.run(command, cwd=directory, capture_output=True, timeout=10).returncode, 0)
        result = self.scanner(directory)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(token, result.stdout + result.stderr)

    def test_secret_in_artifact_filename_fails_without_exception(self):
        directory = self.root / "path-secret"
        directory.mkdir()
        token = "ghp_" + "".join(random.Random(41).sample(string.ascii_letters + string.digits, 36))
        (directory / token).write_text("Harmless content.\n")
        result = self.scanner(directory)
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn(token, result.stdout + result.stderr)

    def test_verified_oci_references_and_sbom_platform_copy(self):
        report = self.export()
        image = self.root / "image"
        self.assertEqual(content.oci_reference(image, report["index"]), "qualification")
        with self.assertRaises(ValueError):
            content.oci_reference(image, "sha256:" + "0" * 64)
        index = json.loads((image / "index.json").read_bytes())
        changed = json.loads(encoded(index))
        changed["manifests"][0]["mediaType"] = "application/octet-stream"
        (image / "index.json").write_bytes(encoded(changed))
        with self.assertRaises(ValueError):
            content.oci_reference(image, report["index"])
        (image / "index.json").write_bytes(encoded(index))
        selected = self.root / "selected-platform"
        shutil.copytree(image, selected)
        inner = json.loads(content.blob(image, index["manifests"][0]))
        platform = next(item for item in inner["manifests"] if item["platform"] == {"os": "linux", "architecture": "amd64"})
        (selected / "index.json").write_bytes(encoded({"schemaVersion": 2, "manifests": [platform]}))
        content.validate_sbom_layout(selected, report["payloads"]["amd64"]["manifest"])
        with self.assertRaises(ValueError):
            content.validate_sbom_layout(selected, report["payloads"]["arm64"]["manifest"])
        platform["mediaType"] = "application/octet-stream"
        (selected / "index.json").write_bytes(encoded({"schemaVersion": 2, "manifests": [platform]}))
        with self.assertRaises(ValueError):
            content.validate_sbom_layout(selected, report["payloads"]["amd64"]["manifest"])

    def test_missing_input_and_private_reference_fail_closed(self):
        self.assertNotEqual(self.scanner(self.root / "absent").returncode, 0)
        directory = self.root / "scan"
        directory.mkdir()
        (directory / "text").write_text("/" + "Users/" + "private-user/project")
        self.assertNotEqual(self.scanner(directory).returncode, 0)

    def test_scanner_rejects_symlinks(self):
        directory = self.root / "scan"
        directory.mkdir()
        (directory / "link").symlink_to(self.root / "elsewhere")
        self.assertNotEqual(self.scanner(directory).returncode, 0)

    def test_private_reference_in_relative_path_fails_without_echo(self):
        directory = self.root / "scan"
        nested = directory / "seeded-private-owner-fixture"
        nested.mkdir(parents=True)
        (nested / "NOTICE").write_text("Harmless content.\\n")
        with self.assertRaises(ValueError):
            content.scan(directory, ["seeded-private-owner-fixture"])

    def test_real_per_architecture_license_collection(self):
        if not shutil.which("go-licenses"):
            self.fail("pinned go-licenses is required, license checks must not skip")
        application, dependency = self.root / "application", self.root / "dependency"
        application.mkdir()
        dependency.mkdir()
        (application / "go.mod").write_text("module example.org/application\n\ngo 1.26.10\n\nrequire example.org/dependency v1.0.0\nreplace example.org/dependency => ../dependency\n")
        (application / "main.go").write_text('package main\nimport "example.org/dependency"\nfunc main() { dependency.Value() }\n')
        (dependency / "go.mod").write_text("module example.org/dependency\n\ngo 1.26.10\n")
        (dependency / "value.go").write_text("package dependency\nfunc Value() {}\n")
        goroot = Path(subprocess.check_output(["go", "env", "GOROOT"], env=dict(os.environ, GOTOOLCHAIN="go1.26.10"), text=True).strip())
        (dependency / "LICENSE").write_bytes((goroot / "LICENSE").read_bytes())
        (dependency / "PATENTS").write_text("An explicit fixture patent notice.\n")
        for arch in ("amd64", "arm64"):
            output = self.root / ("notices-" + arch)
            result = subprocess.run(["sh", str(ROOT / "scripts/collect-licenses.sh"), str(output), "./..."],
                                    cwd=application, env=dict(os.environ, GOARCH=arch), capture_output=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            files = {"licenses/" + path.relative_to(output).as_posix(): path.read_bytes()
                     for path in output.rglob("*") if path.is_file()}
            content.notices(files, arch, MODULE)
            self.assertTrue((output / "example.org/dependency/PATENTS").is_file())
        (dependency / "LICENSE").unlink()
        result = subprocess.run(["sh", str(ROOT / "scripts/collect-licenses.sh"), str(self.root / "unlicensed"), "./..."],
                                cwd=application, env=dict(os.environ, GOARCH="amd64"), capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)

    def test_sdk_declared_license_from_exact_source_commit(self):
        spec = importlib.util.spec_from_file_location("sdk_license", ROOT / "scripts/prepare-sdk-license.py")
        helper = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(helper)
        repo, sdk = self.root / "source", self.root / "sdk"
        repo.mkdir()
        (repo / "shared").mkdir()
        module = "github.com/stellwerk-labs/platform-orchestrator-iam/shared"
        (repo / "LICENSE").write_bytes((ROOT / "LICENSE").read_bytes())
        (repo / "shared/go.mod").write_text("module " + module + "\n\ngo 1.26.10\n")
        (repo / "shared/value.go").write_text("package shared\nfunc Value() {}\n")
        for args in (["init", "-q"], ["add", "."],
                     ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.org",
                      "commit", "-q", "--no-gpg-sign", "-m", "fixture"]):
            subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)
        sha = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
        shutil.copytree(repo / "shared", sdk)
        receipt = helper.prepare(repo, sha, "shared", sdk)
        self.assertEqual(receipt["sourceCommit"], sha)
        application = self.root / "application"
        application.mkdir()
        (application / "go.mod").write_text("module example.org/application\n\ngo 1.26.10\n\nrequire " +
                                          module + " v1.0.0\nreplace " + module + " => ../sdk\n")
        (application / "main.go").write_text('package main\nimport "' + module + '"\nfunc main() { shared.Value() }\n')
        result = subprocess.run(["sh", str(ROOT / "scripts/collect-licenses.sh"), str(self.root / "notices"), "./..."],
                                cwd=application, env=dict(os.environ, GOARCH="amd64"), capture_output=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual((self.root / "notices" / module / "LICENSE").read_bytes(), (ROOT / "LICENSE").read_bytes())
        (sdk / "LICENSE.provenance").unlink()
        result = subprocess.run(["sh", str(ROOT / "scripts/collect-licenses.sh"), str(self.root / "no-receipt"), "./..."],
                                cwd=application, env=dict(os.environ, GOARCH="amd64"), capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        with self.assertRaises(ValueError):
            helper.prepare(repo, sha, "shared", sdk)
        with self.assertRaises(ValueError):
            helper.prepare(repo, sha, "shared", repo / "shared")

    def test_nullable_correction_requires_every_exact_boundary(self):
        directory = self.root / "nullable"
        directory.mkdir()
        header = (ROOT / "scripts/release-notices/nullable-header.txt").read_bytes()
        (directory / "LICENSE").write_bytes(header)
        root = self.root / "collector"
        (root / "scripts/release-notices").mkdir(parents=True)
        helper = root / "scripts/verify-nullable-notice.sh"
        helper.write_bytes((ROOT / "scripts/verify-nullable-notice.sh").read_bytes())
        supplement = root / "scripts/release-notices/apache-2.0.txt"
        original_supplement = (ROOT / "scripts/release-notices/apache-2.0.txt").read_bytes()
        supplement.write_bytes(original_supplement)
        arguments = ["github.com/oapi-codegen/nullable", "v1.2.0", "", str(directory),
                     "h1:VflFkDW980KhBPiFF7nWSyjg+r4Obqj8lXipV0UkP5w="]
        def invoke(args, name):
            return subprocess.run(["sh", str(helper), *args, str(self.root / name)],
                                  capture_output=True, timeout=10)
        self.assertEqual(invoke(arguments, "valid").returncode, 0)
        self.assertEqual((self.root / "valid/LICENSE").read_bytes(), header)
        self.assertEqual((self.root / "valid/LICENSE-2.0.txt").read_bytes(), original_supplement)
        for index, value in ((0, arguments[0] + "-fork"), (0, arguments[0] + "/v2"),
                             (1, "v1.1.0"), (2, "local"), (2, "remote"), (4, "wrong-sum")):
            changed = list(arguments)
            changed[index] = value
            self.assertNotEqual(invoke(changed, "wrong-" + str(index)).returncode, 0)
        (directory / "LICENSE").write_bytes(header + b"changed")
        self.assertNotEqual(invoke(arguments, "wrong-header").returncode, 0)
        (directory / "LICENSE").write_bytes(header)
        supplement.write_bytes(original_supplement + b"changed")
        self.assertNotEqual(invoke(arguments, "wrong-supplement").returncode, 0)

    def test_real_nullable_production_graph_keeps_original_and_supplement(self):
        application = self.root / "application"
        application.mkdir()
        (application / "go.mod").write_text("module example.org/application\n\ngo 1.26.10\n\nrequire github.com/oapi-codegen/nullable v1.2.0\n")
        (application / "go.sum").write_text("github.com/oapi-codegen/nullable v1.2.0 h1:VflFkDW980KhBPiFF7nWSyjg+r4Obqj8lXipV0UkP5w=\n"
                                           "github.com/oapi-codegen/nullable v1.2.0/go.mod h1:KUZ3vUzkmEKY90ksAmit2+5juDIhIZhfDl+0PwOQlFY=\n")
        (application / "main.go").write_text('package main\nimport _ "github.com/oapi-codegen/nullable"\nfunc main() {}\n')
        output = self.root / "notices"
        result = subprocess.run(["sh", str(ROOT / "scripts/collect-licenses.sh"), str(output), "./..."],
                                cwd=application, env=dict(os.environ, GOARCH="amd64"), capture_output=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual((output / "github.com/oapi-codegen/nullable/LICENSE").read_bytes(),
                         (ROOT / "scripts/release-notices/nullable-header.txt").read_bytes())
        self.assertEqual((output / "github.com/oapi-codegen/nullable/LICENSE-2.0.txt").read_bytes(),
                         (ROOT / "scripts/release-notices/apache-2.0.txt").read_bytes())


if __name__ == "__main__":
    unittest.main()
