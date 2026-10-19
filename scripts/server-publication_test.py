#!/usr/bin/env python3
"""Real filesystem, Git and verified-statement fixtures. Never invokes publication."""
import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("publication", ROOT / "scripts/server-publication.py")
publication = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(publication)
MODULE = (ROOT / "go.mod").read_text().splitlines()[0]
REPOSITORY = MODULE.removeprefix("module github.com/")
DIGEST = "sha256:" + "a" * 64


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / "source"
        self.source.mkdir()
        (self.source / "go.mod").write_text(MODULE + "\n")
        workflow = self.source / ".github/workflows/ci.yaml"
        workflow.parent.mkdir(parents=True)
        workflow.write_text("name: fixture\n")
        def git(*args):
            return subprocess.check_output(["git", "-C", str(self.source), *args], text=True,
                                           stderr=subprocess.DEVNULL).strip()
        self.git = git
        git("init", "-b", "main")
        git("config", "user.name", "Publication fixture")
        git("config", "user.email", "fixture@example.invalid")
        git("config", "commit.gpgsign", "false")
        git("add", ".")
        git("commit", "-m", "initial fixture")
        sha = git("rev-parse", "HEAD")
        self.env = {"GITHUB_REPOSITORY": REPOSITORY, "GITHUB_REF": "refs/heads/main",
                    "GITHUB_WORKFLOW_REF": REPOSITORY + "/.github/workflows/ci.yaml@refs/heads/main",
                    "GITHUB_EVENT_NAME": "workflow_dispatch", "GITHUB_SHA": sha,
                    "RELEASE_SOURCE_SHA": sha, "GITHUB_WORKFLOW_SHA": sha,
                    "GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1",
                    "RELEASE_VERSION": "v3.0.0-rc.1", "RELEASE_KIND": "release",
                    "IMAGE_TAG": "ghcr.io/" + REPOSITORY + ":v3.0.0-rc.1", "IMAGE_DIGEST": DIGEST}
        self.layout, self.output = self.root / "layout", self.root / "evidence"
        self.output.mkdir()
        self.report = {"index": DIGEST, "payloads": {}}
        for arch in ("amd64", "arm64"):
            manifest = "sha256:" + ("b" if arch == "amd64" else "c") * 64
            raw = json.dumps({"config": {"Labels": {
                "org.opencontainers.image.source": "https://github.com/" + REPOSITORY,
                "org.opencontainers.image.revision": sha,
                "org.opencontainers.image.version": "v3.0.0-rc.1"}}}).encode()
            config = "sha256:" + hashlib.sha256(raw).hexdigest()
            target = self.layout / "blobs/sha256" / config[7:]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
            self.report["payloads"][arch] = {"manifest": manifest, "config": config}
            write(self.output / (arch + ".spdx.json"), {
                "spdxVersion": "SPDX-2.3", "SPDXID": "SPDXRef-DOCUMENT",
                "documentNamespace": "https://stellwerk.dev/sbom/" + manifest.replace(":", "/") + "/linux/" + arch})
        write(self.output / "platforms.json", self.report)
        write(self.output / "reproducibility.json", {"payloadsIdentical": True})

    def stage(self):
        publication.stage(self.layout, self.output, self.env, self.source)

    def verify(self):
        publication.verify(self.layout, self.output, self.env, self.source)

    def test_real_git_staging_and_checksum_bound_evidence(self):
        self.stage()
        self.verify()
        predicate = json.loads((self.output / "source-provenance.json").read_text())
        self.assertEqual(predicate["buildDefinition"]["resolvedDependencies"][0]["digest"],
                         {"gitCommit": self.env["RELEASE_SOURCE_SHA"]})
        (self.output / "reproducibility.json").write_text("{}")
        with self.assertRaises(ValueError):
            self.verify()

    def test_manual_candidate_stages_from_its_exact_branch_workflow(self):
        ref = "refs/heads/release/candidate-fixture/nested"
        self.git("checkout", "-b", ref.removeprefix("refs/heads/"))
        self.env.update(GITHUB_REF=ref, GITHUB_WORKFLOW_REF=REPOSITORY + "/.github/workflows/ci.yaml@" + ref)
        self.stage()
        self.verify()
        predicate = json.loads((self.output / "source-provenance.json").read_text())
        self.assertEqual(predicate["runDetails"]["builder"]["id"],
                         "https://github.com/" + self.env["GITHUB_WORKFLOW_REF"])
        self.assertEqual(predicate["runDetails"]["metadata"]["invocationId"],
                         "https://github.com/" + REPOSITORY + "/actions/runs/123/attempts/1")

    def test_distinct_candidate_and_producer_commits_require_identical_workflow_bytes(self):
        source = self.env["RELEASE_SOURCE_SHA"]
        ref = "refs/heads/release/candidate-producer"
        self.git("checkout", "-b", ref.removeprefix("refs/heads/"))
        (self.source / "producer.txt").write_text("same workflow, later producer\n")
        self.git("add", "producer.txt")
        self.git("commit", "-m", "producer with identical workflow")
        producer = self.git("rev-parse", "HEAD")
        self.assertNotEqual(source, producer)
        self.git("checkout", "--detach", source)
        self.env.update(GITHUB_REF=ref, GITHUB_WORKFLOW_REF=REPOSITORY + "/.github/workflows/ci.yaml@" + ref,
                        GITHUB_SHA=producer, GITHUB_WORKFLOW_SHA=producer)
        self.stage()
        self.verify()
        predicate = json.loads((self.output / "source-provenance.json").read_text())
        self.assertEqual([entry["digest"]["gitCommit"] for entry in
                          predicate["buildDefinition"]["resolvedDependencies"]], [source, producer])
        (self.output / "source-provenance.json").unlink()
        (self.output / "SHA256SUMS").unlink()
        self.git("checkout", ref.removeprefix("refs/heads/"))
        (self.source / ".github/workflows/ci.yaml").write_text("name: different producer\n")
        self.git("add", ".github/workflows/ci.yaml")
        self.git("commit", "-m", "producer with different workflow")
        changed = self.git("rev-parse", "HEAD")
        self.git("checkout", "--detach", source)
        self.env.update(GITHUB_SHA=changed, GITHUB_WORKFLOW_SHA=changed)
        with self.assertRaises(ValueError):
            self.stage()
        self.assertFalse((self.output / "source-provenance.json").exists())

    def test_matching_nonbranch_or_malformed_refs_are_refused(self):
        for ref in ("refs/tags/v3.0.0-rc.1", "refs/pull/1/merge", "refs/heads/",
                    "refs/heads/release/../candidate", "refs/heads/release//candidate",
                    "refs/heads/release/", "refs/heads/release/candidate.lock",
                    "refs/heads/release/bad name", "refs/heads/release/bad\nname",
                    "refs/heads/release/@{1}", "refs/heads/.hidden"):
            changed = dict(self.env, GITHUB_REF=ref,
                           GITHUB_WORKFLOW_REF=REPOSITORY + "/.github/workflows/ci.yaml@" + ref)
            with self.subTest(ref=ref), self.assertRaises(ValueError):
                publication.identity(changed, MODULE)

    def test_candidate_branch_workflow_binding_is_exact(self):
        ref = "refs/heads/release/candidate-fixture"
        candidate = dict(self.env, GITHUB_REF=ref,
                         GITHUB_WORKFLOW_REF=REPOSITORY + "/.github/workflows/ci.yaml@" + ref)
        publication.identity(candidate, MODULE)
        for workflow in (REPOSITORY + "/.github/workflows/ci.yaml@refs/heads/main",
                         REPOSITORY + "/.github/workflows/other.yaml@" + ref,
                         "other/repository/.github/workflows/ci.yaml@" + ref):
            with self.subTest(workflow=workflow), self.assertRaises(ValueError):
                publication.identity(dict(candidate, GITHUB_WORKFLOW_REF=workflow), MODULE)

    def test_seed_cannot_change_repository_source_or_event_kind(self):
        repository = "stellwerk-labs/platform-orchestrator-dp"
        seed = dict(self.env, GITHUB_REPOSITORY=repository,
                    GITHUB_WORKFLOW_REF=repository + "/.github/workflows/ci.yaml@refs/heads/main",
                    RELEASE_KIND="seed", RELEASE_VERSION="ci-seed-" + self.env["RELEASE_SOURCE_SHA"],
                    IMAGE_TAG="ghcr.io/" + repository + ":ci-seed-" + self.env["RELEASE_SOURCE_SHA"])
        module = "module github.com/" + repository
        publication.identity(seed, module)
        for changed in (dict(seed, RELEASE_VERSION="ci-seed-" + "e" * 40),
                        dict(seed, GITHUB_EVENT_NAME="push")):
            with self.assertRaises(ValueError):
                publication.identity(changed, module)
        repository = "stellwerk-labs/platform-orchestrator-cp"
        changed = dict(seed, GITHUB_REPOSITORY=repository,
                       GITHUB_WORKFLOW_REF=repository + "/.github/workflows/ci.yaml@refs/heads/main",
                       IMAGE_TAG="ghcr.io/" + repository + ":" + seed["RELEASE_VERSION"])
        with self.assertRaises(ValueError):
            publication.identity(changed, "module github.com/" + repository)

    def test_matching_nonmain_push_is_refused(self):
        stable = dict(self.env, GITHUB_EVENT_NAME="push", RELEASE_VERSION="v3.0.0",
                      IMAGE_TAG="ghcr.io/" + REPOSITORY + ":v3.0.0")
        publication.identity(stable, MODULE)
        ref = "refs/heads/release/candidate-fixture"
        stable.update(GITHUB_REF=ref, GITHUB_WORKFLOW_REF=REPOSITORY + "/.github/workflows/ci.yaml@" + ref)
        with self.assertRaises(ValueError):
            publication.identity(stable, MODULE)

    def test_dp_seed_stages_from_branch_with_one_unqualified_platform(self):
        repository = "stellwerk-labs/platform-orchestrator-dp"
        if MODULE != "module github.com/" + repository:
            (self.source / "go.mod").write_text("module github.com/" + repository + "\n")
            self.git("add", "go.mod")
            self.git("commit", "-m", "DP seed fixture")
        source = self.git("rev-parse", "HEAD")
        ref = "refs/heads/release/seed-fixture"
        self.git("checkout", "-b", ref.removeprefix("refs/heads/"))
        self.env.update(GITHUB_REPOSITORY=repository, GITHUB_REF=ref,
                        GITHUB_WORKFLOW_REF=repository + "/.github/workflows/ci.yaml@" + ref,
                        GITHUB_SHA=source, RELEASE_SOURCE_SHA=source, GITHUB_WORKFLOW_SHA=source,
                        RELEASE_KIND="seed", RELEASE_VERSION="ci-seed-" + source,
                        IMAGE_TAG="ghcr.io/" + repository + ":ci-seed-" + source)
        payload = self.report["payloads"]["amd64"]
        labels = {"org.opencontainers.image.source": "https://github.com/" + repository,
                  "org.opencontainers.image.revision": source,
                  "org.opencontainers.image.description": "CI dependency seed; not a qualified release"}
        raw = json.dumps({"config": {"Labels": labels}}).encode()
        payload["config"] = "sha256:" + hashlib.sha256(raw).hexdigest()
        (self.layout / "blobs/sha256" / payload["config"][7:]).write_bytes(raw)
        del self.report["payloads"]["arm64"]
        (self.output / "arm64.spdx.json").unlink()
        write(self.output / "platforms.json", self.report)
        self.stage()
        self.verify()
        predicate = json.loads((self.output / "source-provenance.json").read_text())
        self.assertEqual(predicate["buildDefinition"]["externalParameters"]["purpose"],
                         "unqualified-ci-dependency-seed")

    def test_public_workflow_destination_source_and_run_fail_closed(self):
        publication.identity(self.env, MODULE)
        for key, value in (("GITHUB_REPOSITORY", "private/example"), ("GITHUB_REF", "refs/heads/other"),
                           ("GITHUB_WORKFLOW_REF", self.env["GITHUB_WORKFLOW_REF"] + "/other"),
                           ("GITHUB_EVENT_NAME", "pull_request"), ("RELEASE_SOURCE_SHA", "bad"),
                           ("GITHUB_WORKFLOW_SHA", ""), ("GITHUB_RUN_ID", "0"),
                           ("GITHUB_RUN_ATTEMPT", ""), ("IMAGE_DIGEST", "latest"),
                           ("IMAGE_TAG", self.env["IMAGE_TAG"] + "-other"),
                           ("RELEASE_KIND", "unknown")):
            with self.subTest(key=key), self.assertRaises(ValueError):
                publication.identity(dict(self.env, **{key: value}), MODULE)
        with self.assertRaises(ValueError):
            publication.identity(self.env, "module example.invalid/unknown")

    def test_stable_source_and_manual_candidate_are_not_interchangeable(self):
        stable = dict(self.env, GITHUB_EVENT_NAME="push", RELEASE_VERSION="v3.0.0",
                      IMAGE_TAG="ghcr.io/" + REPOSITORY + ":v3.0.0")
        publication.identity(stable, MODULE)
        for changed in (dict(stable, RELEASE_SOURCE_SHA="d" * 40), dict(self.env, RELEASE_VERSION="v3.0.0"),
                        dict(stable, RELEASE_VERSION="v3.0.0-rc.1")):
            with self.assertRaises(ValueError):
                publication.identity(changed, MODULE)

    def test_dp_seed_has_explicit_unqualified_purpose(self):
        repository = "stellwerk-labs/platform-orchestrator-dp"
        seed = dict(self.env, GITHUB_REPOSITORY=repository,
                    GITHUB_WORKFLOW_REF=repository + "/.github/workflows/ci.yaml@refs/heads/main",
                    RELEASE_KIND="seed", RELEASE_VERSION="ci-seed-" + self.env["RELEASE_SOURCE_SHA"],
                    IMAGE_TAG="ghcr.io/" + repository + ":ci-seed-" + self.env["RELEASE_SOURCE_SHA"])
        name, is_seed = publication.identity(seed, "module github.com/" + repository)
        predicate = publication.provenance(seed, name, is_seed)
        self.assertEqual(predicate["buildDefinition"]["externalParameters"]["purpose"], "unqualified-ci-dependency-seed")
        for changed in (dict(seed, GITHUB_EVENT_NAME="push"), dict(seed, RELEASE_VERSION="v3.0.0")):
            with self.assertRaises(ValueError):
                publication.identity(changed, "module github.com/" + repository)

    def test_checkout_and_workflow_bytes_are_exact(self):
        with self.assertRaises(ValueError):
            publication.stage(self.layout, self.output, dict(self.env, RELEASE_SOURCE_SHA="e" * 40), self.source)
        (self.source / ".github/workflows/ci.yaml").write_text("name: changed\n")
        with self.assertRaises(ValueError):
            self.stage()

    def test_broken_provenance_symlink_never_writes_outside_evidence(self):
        outside = self.root / "outside"
        (self.output / "source-provenance.json").symlink_to(outside)
        with self.assertRaises(ValueError):
            self.stage()
        self.assertFalse(outside.exists())

    def test_index_platform_labels_config_and_sbom_subject_are_exact(self):
        original = (self.output / "platforms.json").read_bytes()
        for change in ("index", "platform"):
            report = copy.deepcopy(self.report)
            if change == "index":
                report["index"] = "sha256:" + "d" * 64
            else:
                del report["payloads"]["arm64"]
            write(self.output / "platforms.json", report)
            with self.assertRaises(ValueError):
                self.stage()
            (self.output / "platforms.json").write_bytes(original)
        sbom = self.output / "amd64.spdx.json"
        old = sbom.read_bytes()
        write(sbom, {"spdxVersion": "SPDX-2.3", "SPDXID": "SPDXRef-DOCUMENT", "documentNamespace": "wrong"})
        with self.assertRaises(ValueError):
            self.stage()
        sbom.write_bytes(old)
        config = self.layout / "blobs/sha256" / self.report["payloads"]["amd64"]["config"][7:]
        config.write_bytes(config.read_bytes() + b" ")
        with self.assertRaises(ValueError):
            self.stage()

    def test_unlisted_symlink_duplicate_checksum_and_changed_provenance_fail(self):
        self.stage()
        checksums = self.output / "SHA256SUMS"
        original = checksums.read_text()
        checksums.write_text(original + original.splitlines()[0] + "\n")
        with self.assertRaises(ValueError):
            self.verify()
        checksums.write_text(original)
        extra = self.output / "unexpected"
        extra.write_text("public")
        with self.assertRaises(ValueError):
            self.verify()
        extra.unlink()
        source = self.output / "source-provenance.json"
        source.unlink()
        source.symlink_to(self.source / "go.mod")
        with self.assertRaises((ValueError, json.JSONDecodeError)):
            self.verify()

    def test_validly_hashed_wrong_source_version_and_source_provenance_fail(self):
        original = copy.deepcopy(self.report)
        raw = (self.layout / "blobs/sha256" / original["payloads"]["amd64"]["config"][7:]).read_bytes()
        for key in ("org.opencontainers.image.source", "org.opencontainers.image.revision",
                    "org.opencontainers.image.version"):
            config = json.loads(raw)
            config["config"]["Labels"][key] = "wrong"
            changed = json.dumps(config).encode()
            digest = hashlib.sha256(changed).hexdigest()
            (self.layout / "blobs/sha256" / digest).write_bytes(changed)
            report = copy.deepcopy(original)
            report["payloads"]["amd64"]["config"] = "sha256:" + digest
            write(self.output / "platforms.json", report)
            with self.subTest(label=key), self.assertRaises(ValueError):
                self.stage()
        write(self.output / "platforms.json", original)
        self.stage()
        with self.assertRaises(ValueError):
            publication.verify(self.layout, self.output, dict(self.env, GITHUB_WORKFLOW_SHA="f" * 40), self.source)

    def test_verified_statements_require_exact_type_subject_and_predicate(self):
        predicate = {"buildDefinition": {"source": self.env["RELEASE_SOURCE_SHA"]}}
        path, proof = self.root / "predicate.json", self.root / "verified.json"
        write(path, predicate)
        statement = {"_type": "https://in-toto.io/Statement/v1", "predicateType": "https://slsa.dev/provenance/v1",
                     "subject": [{"digest": {"sha256": DIGEST[7:]}}], "predicate": predicate}
        def check(value):
            write(proof, {"payload": base64.b64encode(json.dumps(value).encode()).decode()})
            publication.statement(proof, DIGEST, statement["predicateType"], path)
        check(statement)
        for changed in (dict(statement, predicate={}), dict(statement, predicateType="wrong"),
                        dict(statement, subject=[]), dict(statement, subject=statement["subject"] * 2),
                        dict(statement, subject=[{"digest": {"sha256": "f" * 64}}]),
                        dict(statement, _type="wrong")):
            with self.assertRaises(ValueError):
                check(changed)

    def test_existing_publishers_authenticate_before_visibility_without_bypasses(self):
        workflow = (ROOT / ".github/workflows/ci.yaml").read_text()
        jobs = dict(re.findall(r"^  ([a-z-]+):\n(.*?)(?=^  [a-z-]+:\n|\Z)", workflow, re.M | re.S))
        publishers = 0
        for name, job in jobs.items():
            if "oras cp --no-tty --from-oci-layout" not in job:
                self.assertNotIn("id-token: write", job)
                continue
            publishers += 1
            self.assertIn("id-token: write", job)
            namespaces = re.findall(r"BUILDKIT_CACHE_MOUNT_NS=(.+)", job)
            prefix = "server-" + REPOSITORY.rsplit("-", 1)[1] + "-${{ github.run_id }}-${{ github.run_attempt }}-${{ github.job }}-"
            self.assertEqual(namespaces, [prefix + "first", prefix + "second"])
            self.assertNotEqual(namespaces[0], namespaces[1])
            self.assertLess(job.index("server-publication.py stage"), job.index("oras cp --no-tty"))
            self.assertLess(job.index("oras cp --no-tty"), job.index("bash scripts/authenticate-server-artifact.sh"))
            self.assertIn("server-verification-evidence-", job)
            if name in ("release", "candidate-release"):
                self.assertLess(job.index("authenticate-server-artifact.sh"), job.index("gh release edit"))
            if name == "candidate-release":
                self.assertIn("--verify-tag --draft --prerelease --latest=false", job)
            if name == "candidate-seed":
                self.assertIn("RELEASE_KIND: seed", job)
                self.assertNotIn("gh release edit", job)
                self.assertIn("not a qualified release", job)
        self.assertEqual(publishers, 3 if REPOSITORY.endswith("-dp") else 2)
        helper = (ROOT / "scripts/authenticate-server-artifact.sh").read_text()
        for required in ("--certificate-identity", "--certificate-oidc-issuer https://token.actions.githubusercontent.com",
                         "--certificate-github-workflow-sha", "verify-attestation", "server-publication.py",
                         'DOCKER_CONFIG="$anonymous" cosign_release verify', 'source-provenance.json',
                         "spdxjson", "sign-blob", "verify-blob"):
            self.assertIn(required, helper)
        for forbidden in ("--insecure", "--offline", "--check-claims=false", "--key ", "--certificate-identity-regexp"):
            self.assertNotIn(forbidden, helper)
        self.assertLess(helper.index("cosign_release verify-attestation"), helper.index('server-publication.py" statement'))
        self.assertIn("cosign@sha256:9e5c2f2edc34351160407ca3416c61855bdf9403c3c5936e0f0be7fc261611b8",
                      (ROOT / "scripts/publication-tools.sh").read_text())


if __name__ == "__main__":
    unittest.main()
