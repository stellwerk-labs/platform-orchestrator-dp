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

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("release_content", ROOT / "scripts/release_content.py")
content = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(content)
MODULE = "example.org/application"


def encoded(value):
    return json.dumps(value, sort_keys=True).encode()


def layer(entries):
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as archive:
        for name, data in entries.items():
            entry = tarfile.TarInfo(name)
            entry.mode = 0o755 if name.startswith("opt/server/") else 0o644
            if isinstance(data, tuple):
                entry.type, entry.linkname = data
            else:
                entry.size = len(data)
            archive.addfile(entry, None if isinstance(data, tuple) else io.BytesIO(data))
    return raw.getvalue()


def elf(arch):
    data = bytearray(b"\x7fELF\x02\x01" + b"\0" * 60)
    data[18:20] = (62 if arch == "amd64" else 183).to_bytes(2, "little")
    return bytes(data)


def license_bundle(arch):
    files = {"application/LICENSE": b"application license", "go/LICENSE": b"Go license", "go/PATENTS": b"Go patent grant",
             "example.org/dependency/LICENSE": b"dependency license",
             "modules.txt": b"example.org/application|\nexample.org/dependency|v1.0.0\n",
             "target.txt": ("linux/" + arch + "\n").encode()}
    files["manifest.sha256"] = "".join(hashlib.sha256(data).hexdigest() + "  ./" + name + "\n"
                                            for name, data in sorted(files.items())).encode()
    return {"licenses/" + name: data for name, data in files.items()}


def layout(root, extra=None, whiteout=False, mutate=None, binaries=("opt/server/server",), missing_provenance=False):
    root.mkdir()
    (root / "oci-layout").write_bytes(encoded({"imageLayoutVersion": "1.0.0"}))
    blobroot = root / "blobs/sha256"
    blobroot.mkdir(parents=True)
    def store(data, media):
        descriptor = {"mediaType": media, "digest": content.digest(data), "size": len(data)}
        (blobroot / descriptor["digest"][7:]).write_bytes(data)
        return descriptor
    manifests = []
    for arch in ("amd64", "arm64"):
        entries = license_bundle(arch)
        entries.update({name: elf(arch) for name in binaries})
        if extra:
            entries.update(extra)
        raws = [layer(entries)]
        if whiteout:
            raws.append(layer({"opt/server/.wh.removed": b""}))
        config = {"os": "linux", "architecture": arch, "config": {"User": "65532:65532", "Entrypoint": ["/opt/server/server"]},
                  "rootfs": {"type": "layers", "diff_ids": [content.digest(raw) for raw in raws]}}
        image = {"schemaVersion": 2, "config": store(encoded(config), "application/vnd.oci.image.config.v1+json"),
                 "layers": [store(gzip.compress(raw, mtime=0), "application/vnd.oci.image.layer.v1.tar+gzip") for raw in raws]}
        if mutate:
            mutate(arch, image, config, store)
        descriptor = store(encoded(image), "application/vnd.oci.image.manifest.v1+json")
        descriptor["platform"] = {"os": "linux", "architecture": arch}
        manifests.append(descriptor)
        statements = [{"_type": "https://in-toto.io/Statement/v0.1", "subject": [{"digest": {"sha256": descriptor["digest"][7:]}}],
                       "predicateType": kind, "predicate": {}} for kind in ("https://spdx.dev/Document", "https://slsa.dev/provenance/v0.2")]
        if missing_provenance:
            statements.pop()
        attestation = {"schemaVersion": 2, "config": store(b"{}", "application/vnd.oci.image.config.v1+json"),
                       "layers": [store(encoded(statement), "application/vnd.in-toto+json") for statement in statements]}
        attestation_descriptor = store(encoded(attestation), "application/vnd.oci.image.manifest.v1+json")
        attestation_descriptor.update(platform={"os": "unknown", "architecture": "unknown"},
                                      annotations={"vnd.docker.reference.type": "attestation-manifest",
                                                   "vnd.docker.reference.digest": descriptor["digest"]})
        manifests.append(attestation_descriptor)
    descriptor = store(encoded({"schemaVersion": 2, "manifests": manifests}), "application/vnd.oci.image.index.v1+json")
    descriptor["annotations"] = {"org.opencontainers.image.ref.name": "qualification"}
    (root / "index.json").write_bytes(encoded({"schemaVersion": 2, "manifests": [descriptor]}))


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

    def test_platform_bytes_notices_and_attestations(self):
        report = self.export(extra={"etc/link": (tarfile.SYMTYPE, "/etc/certs"), "etc/certs": b"base certificate"})
        self.assertEqual(set(report["payloads"]), {"amd64", "arm64"})
        self.assertEqual(len(report["attestations"]), 2)
        self.assertEqual((self.root / "output/arm64/opt/server/server").read_bytes(), elf("arm64"))
        self.assertFalse((self.root / "output/amd64/etc/link").exists())
        self.assertEqual(self.scanner(self.root / "output").returncode, 0)

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

    def kin_source_pair(self):
        environment = dict(os.environ, GOTOOLCHAIN="go1.26.2", GOWORK="off", GOENV="off")
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
        buildinfo = "fixture: go1.26.2\n\tdep\t" + "\t".join((content.KIN_MODULE, content.KIN_VERSION, content.KIN_SUM)) + "\n"
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
            (data, finding, sums, buildinfo.replace("go1.26.2", "go1.26.3")),
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
        (application / "go.mod").write_text("module example.org/elf-fixture\n\ngo 1.26.2\n\nrequire "
                                           + content.KIN_MODULE + " " + content.KIN_VERSION + "\n")
        (application / "go.sum").write_bytes((ROOT / "go.sum").read_bytes())
        (application / "main.go").write_text('package main\nimport ("fmt"; _ "github.com/getkin/kin-openapi/openapi3")\n'
                                            + "func main() { fmt.Print(" + json.dumps(pair.decode()) + ") }\n")
        directory = self.root / "real-elf-scan"
        directory.mkdir()
        executable = directory / "server"
        environment = dict(os.environ, GOTOOLCHAIN="go1.26.2", GOWORK="off", GOENV="off",
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
        (application / "go.mod").write_text("module example.org/application\n\ngo 1.26.2\n\nrequire example.org/dependency v1.0.0\nreplace example.org/dependency => ../dependency\n")
        (application / "main.go").write_text('package main\nimport "example.org/dependency"\nfunc main() { dependency.Value() }\n')
        (dependency / "go.mod").write_text("module example.org/dependency\n\ngo 1.26.2\n")
        (dependency / "value.go").write_text("package dependency\nfunc Value() {}\n")
        goroot = Path(subprocess.check_output(["go", "env", "GOROOT"], env=dict(os.environ, GOTOOLCHAIN="go1.26.2"), text=True).strip())
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
        (repo / "shared/go.mod").write_text("module " + module + "\n\ngo 1.26.2\n")
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
        (application / "go.mod").write_text("module example.org/application\n\ngo 1.26.2\n\nrequire " +
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
        (application / "go.mod").write_text("module example.org/application\n\ngo 1.26.2\n\nrequire github.com/oapi-codegen/nullable v1.2.0\n")
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
