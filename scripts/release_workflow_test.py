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
    def assert_publisher_buildx_policy(self, workflow):
        publishers = [job for job in re.split(r"(?m)^  [a-z][a-z0-9-]*:\n", workflow)[1:]
                      if "oras cp --no-tty --from-oci-layout" in job]
        self.assertEqual(len(publishers), 3)
        for job in publishers:
            steps = re.split(r"(?m)^      - name: ", job)[1:]
            setup = [step for step in steps if "uses: docker/setup-buildx-action@" in step]
            builds = [step for step in steps if "uses: docker/build-push-action@" in step]
            self.assertEqual(len(setup), 1)
            self.assertIn("uses: docker/setup-buildx-action@37fe631027851001ddb9b187196cc803df7f5f0e", setup[0])
            self.assertIn("          version: v0.38.0\n", setup[0])
            self.assertRegex(setup[0], re.compile(
                r"(?m)^          driver-opts: \|\n"
                r"            image=moby/buildkit@sha256:cec9f139f45e93c5c69c60f8b07cfad9f43f4ef6b6a6cd917527fea5ff2e3dea\n"
                r"            provenance-add-gha=false$"))
            self.assertEqual(len(builds), 2)
            for build in builds:
                self.assertIn("sbom: generator=docker/buildkit-syft-scanner@sha256:ae4f3b554449e7e25548e7d8ccc029d17357348e30c6e3df01b92bc93654d6a9", build)
                self.assertIn("provenance: mode=max", build)

    def assert_integration_generation_precedes_fixture_work(self, workflow):
        integration = workflow.split("\n  integration:\n", 1)[1].split("\n  release:\n", 1)[0]
        steps = re.split(r"(?m)^      - name: ", integration)[1:]
        names = [step.split("\n", 1)[0] for step in steps]
        generation = names.index("Generate integration test prerequisites")
        self.assertLess(names.index("Setup Go"), generation)
        self.assertLess(generation, names.index("Setup Score Compose"))
        self.assertLess(generation, names.index("Run integration tests"))
        self.assertRegex(steps[generation], re.compile(
            r"(?m)^        run: \|\n"
            r"          make generate\n"
            r"          git diff --exit-code$"))

    def test_integration_generates_host_test_prerequisites_before_fixture_work(self):
        workflow = (ROOT / ".github/workflows/ci.yaml").read_text()
        self.assert_integration_generation_precedes_fixture_work(workflow)
        generation = ("      - name: Generate integration test prerequisites\n"
                      "        run: |\n"
                      "          make generate\n"
                      "          git diff --exit-code\n\n")
        missing = workflow.replace(generation, "", 1)
        with self.assertRaises((AssertionError, ValueError)):
            self.assert_integration_generation_precedes_fixture_work(missing)
        reordered = missing.replace("      - name: Show integration test logs\n",
                                    generation + "      - name: Show integration test logs\n", 1)
        with self.assertRaises(AssertionError):
            self.assert_integration_generation_precedes_fixture_work(reordered)

    def test_every_publisher_uses_pinned_buildx_and_creation_context_opt_out(self):
        workflow = (ROOT / ".github/workflows/ci.yaml").read_text()
        self.assert_publisher_buildx_policy(workflow)

    def test_publisher_builder_policy_rejects_tool_context_and_attestation_drift(self):
        workflow = (ROOT / ".github/workflows/ci.yaml").read_text()
        changes = (
            ("version: v0.38.0", "version: v0.37.2"),
            ("provenance-add-gha=false", "provenance-add-gha=true"),
            ("image=moby/buildkit@sha256:cec9f139f45e93c5c69c60f8b07cfad9f43f4ef6b6a6cd917527fea5ff2e3dea",
             "image=moby/buildkit@sha256:" + "0" * 64),
            ("sbom: generator=docker/buildkit-syft-scanner@sha256:ae4f3b554449e7e25548e7d8ccc029d17357348e30c6e3df01b92bc93654d6a9",
             "sbom: true"),
            ("provenance: mode=max", "provenance: mode=min"),
        )
        for old, new in changes:
            with self.subTest(change=old), self.assertRaises(AssertionError):
                self.assert_publisher_buildx_policy(workflow.replace(old, new, 1))

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
            self.assertEqual(job.count("          sbom: generator=docker/buildkit-syft-scanner@sha256:ae4f3b554449e7e25548e7d8ccc029d17357348e30c6e3df01b92bc93654d6a9"), 2)
            self.assertNotIn("          sbom: true", job)
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
            reinspection = job.index('bash scripts/check-server-artifact.sh "$RUNNER_TEMP/server-layout" "$RUNNER_TEMP/server-publication-inspection"')
            exact_report = job.index('cmp "$RUNNER_TEMP/server-inspection/report.json" "$RUNNER_TEMP/server-publication-inspection/report.json"')
            self.assertLess(evidence_scan, reinspection)
            self.assertLess(reinspection, exact_report)
            self.assertLess(exact_report, publication)
            self.assertEqual(job.count("server-publication-inspection/report.json"), 1)
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


    def assert_score_public_route_ownership(self, score):
        self.assertRegex(score, r"(?m)^metadata:\n  name: platform-orchestrator-dp$")
        resources = score.split("\nresources:\n", 1)[1]
        blocks = re.split(r"(?m)^  ([a-z][a-z0-9-]*):\n", resources)
        routes = {}
        for name, body in zip(blocks[1::2], blocks[2::2]):
            if not re.search(r"(?m)^    type: route$", body):
                continue
            paths = re.findall(r"(?m)^      path: (.+)$", body)
            self.assertEqual(len(paths), 1, name)
            routes[name] = re.compile(paths[0])
            if name in ("route", "runner-rollouts-route"):
                self.assertEqual(re.findall(r"(?m)^      host: (.+)$", body),
                                 ["${resources.dns.host}"], name)
                self.assertEqual(re.findall(r"(?m)^      port: (.+)$", body), ["8080"], name)
        for path, owner in (
                ("/orgs/example/deployment-queue", "route"),
                ("/orgs/example/deployment-queue/item", "route"),
                ("/orgs/example/deployment-queue/item/actions/cancel", "route"),
                ("/orgs/example/projects/project/envs/env/runner-rollouts", "runner-rollouts-route"),
                ("/orgs/example/projects/project/envs/env/runner-rollouts/run", "runner-rollouts-route"),
                ("/orgs/example/projects/project/envs/env/runner-rollouts/run/approve", "runner-rollouts-route"),
                ("/orgs/example/projects/project/envs/env/runner-rollouts/run/plan-logs", "runner-rollouts-route")):
            self.assertEqual({name for name, route in routes.items() if route.fullmatch(path)},
                             {owner}, path)
        for path in ("/orgs/example/projects", "/orgs/example/runners",
                     "/orgs/example/projects/project", "/orgs/example/projects/project/envs/env",
                     "/internal/orgs/example/deployment-queue",
                     "/internal/orgs/example/projects/project/envs/env/runner-rollouts",
                     "/orgs/example/deployment-queue-adjacent",
                     "/orgs/example/projects/project/envs/env/runner-rollouts-adjacent"):
            self.assertFalse(any(route.fullmatch(path) for route in routes.values()), path)

    def test_score_routes_public_queue_api_only_to_data_plane(self):
        score = (ROOT / "score.yaml").read_text()
        self.assert_score_public_route_ownership(score)
        self.assert_score_public_route_ownership(score + (
            "  additional-public-route:\n"
            "    type: route\n"
            "    params:\n"
            "      host: ${resources.dns.host}\n"
            "      port: 8080\n"
            "      path: /orgs/[^/]+/public-extension($|/.*)\n"))
        queue = "/orgs/[^/]+/(deployments|deployment-queue|module-test-deployments|last-deployments|active-resources|metadata-keys)($|/.*)"
        rollouts = "/orgs/[^/]+/projects/[^/]+/envs/[^/]+/runner-rollouts($|/.*)"
        endpoint = ("  route:\n    type: route\n    params:\n"
                    "      host: ${resources.dns.host}\n      port: 8080\n")
        changes = (
            ("missing queue", "  route:\n    type: route\n", "  route:\n    type: service\n"),
            ("missing rollouts", "  runner-rollouts-route:\n    type: route\n",
             "  runner-rollouts-route:\n    type: service\n"),
            ("overlap", rollouts, queue),
            ("Core paths", queue, "/orgs/.*"),
            ("queue boundary", queue, queue.removesuffix("($|/.*)") + ".*"),
            ("rollout boundary", rollouts, rollouts.removesuffix("($|/.*)") + ".*"),
            ("public host", endpoint, endpoint.replace("${resources.dns.host}", "${resources.dns-oidc.host}")),
            ("public port", endpoint, endpoint.replace("port: 8080", "port: 8081")),
            ("workload owner", "  name: platform-orchestrator-dp\n", "  name: platform-orchestrator-cp\n"),
        )
        for reason, old, new in changes:
            with self.subTest(change=reason):
                self.assertEqual(score.count(old), 1)
                with self.assertRaises(AssertionError):
                    self.assert_score_public_route_ownership(score.replace(old, new, 1))


if __name__ == "__main__":
    unittest.main()
