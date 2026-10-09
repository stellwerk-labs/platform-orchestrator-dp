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


def layout(root, extra=None, whiteout=False, mutate=None, binaries=("opt/server/server",)):
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
        attestation = {"schemaVersion": 2, "config": store(b"{}", "application/vnd.oci.image.config.v1+json"),
                       "layers": [store(encoded(statement), "application/vnd.in-toto+json") for statement in statements]}
        attestation_descriptor = store(encoded(attestation), "application/vnd.oci.image.manifest.v1+json")
        attestation_descriptor.update(platform={"os": "unknown", "architecture": "unknown"},
                                      annotations={"vnd.docker.reference.type": "attestation-manifest",
                                                   "vnd.docker.reference.digest": descriptor["digest"]})
        manifests.append(attestation_descriptor)
    descriptor = store(encoded({"schemaVersion": 2, "manifests": manifests}), "application/vnd.oci.image.index.v1+json")
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

    def scanner(self, directory):
        return subprocess.run(["bash", str(ROOT / "scripts/scan-release-content.sh"), str(directory)],
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


if __name__ == "__main__":
    unittest.main()
