#!/usr/bin/env python3
"""Verify stable publication prerequisites without publishing anything."""
import argparse
import importlib.util
from pathlib import Path
import re
import subprocess

spec = importlib.util.spec_from_file_location("candidate", Path(__file__).with_name("candidate-release.py"))
candidate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(candidate)

STABLE = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
SHA = re.compile(r"[0-9a-f]{40}")


def validate_stable_identity(tag, expected_sha, head_sha, tag_sha):
    if not STABLE.fullmatch(tag):
        raise ValueError("stable tag must be canonical vX.Y.Z")
    if not SHA.fullmatch(expected_sha):
        raise ValueError("stable source must be a complete lowercase commit SHA")
    if expected_sha != head_sha or expected_sha != tag_sha:
        raise ValueError("stable tag, event source and checkout must identify the same commit")


def validate_release_notes(tag):
    notes = Path("docs/releases", tag + ".md")
    if not notes.is_file() or not notes.read_text().strip():
        raise ValueError("reviewed stable release notes must be nonempty")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("sha")
    args = parser.parse_args()
    validate_stable_identity(args.tag, args.sha, args.sha, args.sha)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    tag = subprocess.check_output(
        ["git", "rev-parse", "--verify", f"refs/tags/{args.tag}^{{commit}}"], text=True
    ).strip()
    validate_stable_identity(args.tag, args.sha, head, tag)
    validate_release_notes(args.tag)
    candidate.assert_release_unreserved(args.tag)
    candidate.assert_image_absent(candidate.REPOSITORY, args.tag)
    print(f"Stable publication prerequisites verified for {args.tag}; no writes performed.")


if __name__ == "__main__":
    main()
