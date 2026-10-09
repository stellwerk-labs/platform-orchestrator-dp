#!/usr/bin/env python3
"""Fail-closed publication wiring contracts, without invoking a publisher."""
from pathlib import Path
import re
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


def component(directory):
    module = (directory / "go.mod").read_text().splitlines()[0]
    known = {"module github.com/stellwerk-labs/platform-orchestrator-" + name: name
             for name in ("cp", "dp", "iam")}
    if module not in known:
        raise ValueError("unknown server module identity")
    return known[module]


class WorkflowTests(unittest.TestCase):
    def test_every_image_publisher_validates_complete_local_layout_first(self):
        workflow = (ROOT / ".github/workflows/ci.yaml").read_text()
        publishers = 0
        for job in re.split(r"(?m)^  [a-z][a-z0-9-]+:\n", workflow)[1:]:
            if "oras cp --no-tty --from-oci-layout" not in job:
                continue
            publishers += 1
            self.assertIn("github.repository == 'stellwerk-labs/", job)
            self.assertIn("persist-credentials: false", job)
            self.assertNotIn("          push: true", job)
            self.assertEqual(job.count("          push: false"), 2)
            self.assertEqual(job.count("          sbom: true"), 2)
            self.assertEqual(job.count("          provenance: mode=max"), 2)
            self.assertEqual(job.count("rewrite-timestamp=true"), 2)
            self.assertEqual(job.count("SOURCE_DATE_EPOCH=${{ env.SOURCE_DATE_EPOCH }}"), 2)
            self.assertIn("no-cache: true", job)
            gate = job.index("bash scripts/check-server-artifact.sh")
            sbom = job.index("bash scripts/generate-server-sbom.sh")
            comparison = job.index("python3 scripts/release_content.py compare")
            publication = job.index("oras cp --no-tty --from-oci-layout")
            self.assertLess(gate, comparison)
            self.assertLess(comparison, sbom)
            self.assertLess(sbom, publication)
            evidence_scan = job.index('bash scripts/scan-release-content.sh "$RUNNER_TEMP/server-evidence"')
            self.assertLess(sbom, evidence_scan)
            self.assertLess(evidence_scan, publication)
            self.assertIn("server-evidence/", job)
            self.assertIn('test "$(oras resolve "$IMAGE_TAG")" = "$IMAGE_DIGEST"', job)
            self.assertIn('python3 scripts/release_content.py oci-reference "$RUNNER_TEMP/server-layout" "$IMAGE_DIGEST"', job)
            self.assertIn('"$RUNNER_TEMP/server-layout:$OCI_REFERENCE" "$IMAGE_TAG"', job)
            self.assertNotIn('"$RUNNER_TEMP/server-layout@$IMAGE_DIGEST"', job)
            self.assertIn("moby/buildkit@sha256:cec9f139f45e93c5c69c60f8b07cfad9f43f4ef6b6a6cd917527fea5ff2e3dea", job)
        expected = 3 if component(ROOT) == "dp" else 2
        self.assertEqual(publishers, expected)

    def test_production_dockerfile_carries_notices_and_all_executables(self):
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertEqual(len(re.findall(r"(?m)^FROM .*@sha256:[0-9a-f]{64}", dockerfile)), 2)
        self.assertIn("/opt/licenses /licenses", dockerfile)
        self.assertNotIn("apk add", dockerfile)
        build = (ROOT / "scripts/build-server-artifact.sh").read_text()
        self.assertIn("-trimpath -buildvcs=false", build)
        self.assertIn("GOAMD64=v1 GOARM64=v8.0", build)
        if component(ROOT) == "iam":
            self.assertIn("./cmd/authorization-migrate", build)
            self.assertIn("opt/server/authorization-migrate", (ROOT / "scripts/check-server-artifact.sh").read_text())

    def test_installer_actual_version_expression_is_strict(self):
        import subprocess
        installer = (ROOT / "scripts/install-oras.sh").read_text()
        patterns = re.findall(r"grep -E '([^']+)'", installer)
        self.assertEqual(len(patterns), 1)
        for version, accepted in (("1.3.4", True), ("1x3x4", False), ("1.3.5", False),
                                  ("1.3.4+Homebrew", False), ("11.3.4", False)):
            result = subprocess.run(["grep", "-E", patterns[0]], input="Version:\t" + version + "\n",
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode == 0, accepted, version)

    def test_neutral_checkout_directory_preserves_component_contracts(self):
        with tempfile.TemporaryDirectory() as temporary:
            checkout = Path(temporary) / "source"
            checkout.mkdir()
            (checkout / "go.mod").write_bytes((ROOT / "go.mod").read_bytes())
            self.assertEqual(component(checkout), component(ROOT))
            (checkout / "go.mod").write_text("module example.org/unknown\\n")
            with self.assertRaises(ValueError):
                component(checkout)


if __name__ == "__main__":
    unittest.main()
