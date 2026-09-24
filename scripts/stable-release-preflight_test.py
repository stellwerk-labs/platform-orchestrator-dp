#!/usr/bin/env python3
import importlib.util
import json
import os
from pathlib import Path
import re
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("stable", Path(__file__).with_name("stable-release-preflight.py"))
stable = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stable)
SHA = "a" * 40


class StableReleaseTests(unittest.TestCase):
    def test_exact_canonical_stable_source(self):
        stable.validate_stable_identity("v3.2.0", SHA, SHA, SHA)
        for tag in ("v03.2.0", "v3.2.0-rc.1", "latest", "v3.2.0+build", "../notes"):
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                stable.validate_stable_identity(tag, SHA, SHA, SHA)
        for values in ((SHA, "b" * 40, SHA), (SHA, SHA, "b" * 40), ("abc", "abc", "abc")):
            with self.subTest(values=values), self.assertRaises(ValueError):
                stable.validate_stable_identity("v3.2.0", *values)

    def test_reviewed_notes_are_nonempty_in_checked_out_source(self):
        previous_directory = Path.cwd()
        with tempfile.TemporaryDirectory() as temporary_directory:
            notes = Path(temporary_directory, "docs/releases")
            notes.mkdir(parents=True)
            (notes / "v3.2.0.md").write_text("Reviewed notes.\n")
            (notes / "v3.2.1.md").write_text("")
            (notes / "v3.2.2.md").write_text(" \n\t")
            try:
                os.chdir(temporary_directory)
                stable.validate_release_notes("v3.2.0")
                for tag in ("v3.2.1", "v3.2.2", "v3.2.3"):
                    with self.subTest(tag=tag), self.assertRaises(ValueError):
                        stable.validate_release_notes(tag)
            finally:
                os.chdir(previous_directory)

    def test_publication_destinations_fail_closed(self):
        stable.candidate.validate_absence_status(404)
        for status in (200, 401, 403, 429, 500, 503):
            with self.subTest(status=status), self.assertRaises(ValueError):
                stable.candidate.validate_absence_status(status)
        for draft in (True, False):
            with self.subTest(draft=draft), self.assertRaises(ValueError):
                stable.candidate.validate_release_page(
                    "v3.2.0", [{"tag_name": "v3.2.0", "draft": draft}]
                )

    def test_semantic_release_only_assigns_versions_with_existing_rules(self):
        config = json.loads(Path(__file__).parents[1].joinpath(".releaserc.json").read_text())
        self.assertEqual([plugin[0] for plugin in config["plugins"]], ["@semantic-release/commit-analyzer"])
        rules = config["plugins"][0][1]["releaseRules"]
        self.assertEqual(
            rules,
            [
                {"breaking": True, "release": "major"},
                {"revert": True, "release": "patch"},
                {"type": "feat", "release": "minor"},
                {"type": "fix", "release": "patch"},
                {"type": "build", "scope": "deps", "release": "patch"},
                {"type": "*", "release": "patch"},
            ],
        )

    def test_workflow_reserves_builds_verifies_then_publishes(self):
        text = Path(__file__).parents[1].joinpath(".github/workflows/ci.yaml").read_text()
        jobs = dict(re.findall(r"^  ([a-z-]+):\n(.*?)(?=^  [a-z-]+:\n|\Z)", text, re.M | re.S))
        release = jobs["release"]
        image_step = next(
            step for step in release.split("\n      - ") if "uses: docker/build-push-action@" in step
        )
        for required in (
            "python3 scripts/stable-release-preflight.py",
            "gh release create \"$RELEASE_TAG\" --verify-tag --draft --latest=false",
            "gh release edit \"$RELEASE_TAG\" --draft=false --latest",
            "context: .",
            "sbom: true",
            "provenance: mode=max",
            "org.opencontainers.image.version=${{ steps.release.outputs.version }}",
            "org.opencontainers.image.revision=${{ github.sha }}",
            "docker buildx imagetools inspect",
        ):
            self.assertIn(required, release)
        self.assertIn("sbom: true", image_step)
        self.assertIn("provenance: mode=max", image_step)
        self.assertLess(release.index("stable-release-preflight.py"), release.index("gh release create"))
        self.assertLess(release.index("gh release create"), release.index("docker/build-push-action@"))
        self.assertLess(release.index("docker/build-push-action@"), release.index("imagetools inspect"))
        self.assertLess(release.index("imagetools inspect"), release.index("gh release edit"))
        self.assertIn("scripts/test-release-versioning.sh", jobs["verify"])


if __name__ == "__main__":
    unittest.main()
