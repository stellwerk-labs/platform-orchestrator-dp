#!/usr/bin/env python3
"""Actual pinned Cosign cryptography, disposable keys, no network or registry writes."""
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("publication", ROOT / "scripts/server-publication.py")
publication = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publication)
IMAGE = re.search(r"^cosign_image=(\S+)", (ROOT / "scripts/publication-tools.sh").read_text(), re.M)[1]


class OfflineCosignTests(unittest.TestCase):
    def test_real_signed_blob_and_attestation_reject_tampering_and_wrong_key(self):
        result = subprocess.run(["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"],
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, "pinned Cosign image must already be present; no automatic pull")
        with tempfile.TemporaryDirectory(prefix="server-cosign-") as temporary:
            directory = Path(temporary)
            (directory / "signing-config.json").write_text(json.dumps({
                "mediaType": "application/vnd.dev.sigstore.signingconfig.v0.2+json"}))
            blob = directory / "manifest.json"
            blob.write_text('{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}\n')
            predicate = directory / "predicate.json"
            predicate.write_text(json.dumps({"buildDefinition": {"buildType": "offline-cryptographic-test",
                                 "resolvedDependencies": [{"digest": {"gitCommit": "a" * 40}}]}}))
            def cosign(*args, accepted=True):
                command = ["docker", "run", "--rm", "--pull=never", "--network", "none",
                           "--cpus", "0.5", "--memory", "256m", "--pids-limit", "64", "--read-only",
                           "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                           "--user", str(os.getuid()) + ":" + str(os.getgid()),
                           "--tmpfs", "/tmp:rw,noexec,nosuid,size=32m,mode=1777",
                           "--env", "HOME=/tmp", "--env", "COSIGN_PASSWORD=",
                           "--mount", "type=bind,src=" + temporary + ",dst=/fixture",
                           "--workdir", "/fixture", IMAGE, *args]
                result = subprocess.run(command, capture_output=True, timeout=45)
                self.assertEqual(result.returncode == 0, accepted, "offline Cosign " + args[0])
            for key in ("publisher", "wrong"):
                cosign("generate-key-pair", "--output-key-prefix", "/fixture/" + key)
            cosign("sign-blob", "--yes", "--signing-config=/fixture/signing-config.json",
                   "--key=/fixture/publisher.key", "--bundle=/fixture/blob.sigstore.json", "/fixture/manifest.json")
            # Local test-only trust: network-none plus no-log managed keys. None of
            # these verification exceptions is permitted in the production helper.
            verify = ("verify-blob", "--insecure-ignore-tlog", "--key=/fixture/publisher.pub",
                      "--bundle=/fixture/blob.sigstore.json", "/fixture/manifest.json")
            cosign(*verify)
            cosign(*[arg.replace("publisher.pub", "wrong.pub") for arg in verify], accepted=False)
            original = blob.read_bytes()
            (directory / "changed.json").write_bytes(original + b" ")
            changed_blob = [arg.replace("/fixture/manifest.json", "/fixture/changed.json") for arg in verify]
            cosign(*changed_blob, accepted=False)
            cosign("attest-blob", "--yes", "--signing-config=/fixture/signing-config.json",
                   "--key=/fixture/publisher.key", "--bundle=/fixture/provenance.sigstore.json",
                   "--type=slsaprovenance1", "--predicate=/fixture/predicate.json", "/fixture/manifest.json")
            attest_verify = ("verify-blob-attestation", "--insecure-ignore-tlog", "--key=/fixture/publisher.pub",
                             "--type=slsaprovenance1", "--bundle=/fixture/provenance.sigstore.json",
                             "/fixture/manifest.json")
            cosign(*attest_verify)
            bundle = directory / "provenance.sigstore.json"
            digest = "sha256:" + hashlib.sha256(original).hexdigest()
            publication.statement(bundle, digest, "https://slsa.dev/provenance/v1", predicate)
            spdx = directory / "spdx.json"
            spdx.write_text(json.dumps({"spdxVersion": "SPDX-2.3", "SPDXID": "SPDXRef-DOCUMENT",
                "documentNamespace": "https://example.invalid/sbom/" + digest[7:],
                "packages": [{"SPDXID": "SPDXRef-test", "name": "offline-fixture", "versionInfo": "1.0.0"}]}))
            cosign("attest-blob", "--yes", "--signing-config=/fixture/signing-config.json",
                   "--key=/fixture/publisher.key", "--bundle=/fixture/spdx.sigstore.json",
                   "--type=spdxjson", "--predicate=/fixture/spdx.json", "/fixture/manifest.json")
            cosign("verify-blob-attestation", "--insecure-ignore-tlog", "--key=/fixture/publisher.pub",
                   "--type=spdxjson", "--bundle=/fixture/spdx.sigstore.json", "/fixture/manifest.json")
            publication.statement(directory / "spdx.sigstore.json", digest, "https://spdx.dev/Document", spdx)
            public = directory / "shipping"
            public.mkdir()
            for name in ("blob.sigstore.json", "provenance.sigstore.json", "spdx.sigstore.json"):
                shutil.copyfile(directory / name, public / name)
            scanned = subprocess.run(["bash", str(ROOT / "scripts/scan-release-content.sh"), str(public)],
                                     capture_output=True, timeout=45)
            self.assertEqual(scanned.returncode, 0, "real public signature/attestation bundles must pass shipping scan")
            cosign(*[arg.replace("publisher.pub", "wrong.pub") for arg in attest_verify], accepted=False)
            cosign(*[arg.replace("type=slsaprovenance1", "type=spdxjson") for arg in attest_verify], accepted=False)
            cosign(*[arg.replace("/fixture/manifest.json", "/fixture/changed.json") for arg in attest_verify], accepted=False)
            data = json.loads(bundle.read_text())
            statement = json.loads(base64.b64decode(data["dsseEnvelope"]["payload"]))
            statement["predicate"] = {}
            data["dsseEnvelope"]["payload"] = base64.b64encode(json.dumps(statement).encode()).decode()
            (directory / "changed.sigstore.json").write_text(json.dumps(data))
            cosign(*[arg.replace("/fixture/provenance.sigstore.json", "/fixture/changed.sigstore.json")
                     for arg in attest_verify], accepted=False)


if __name__ == "__main__":
    unittest.main()
