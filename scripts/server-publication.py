#!/usr/bin/env python3
"""Bind server publication evidence to the exact source, workflow and OCI subjects."""
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SHA = re.compile(r"[0-9a-f]{40}")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
VERSION = re.compile(r"v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-rc\.[1-9][0-9]*)?")


def identity(env, module):
    known = {"module github.com/stellwerk-labs/platform-orchestrator-" + name:
             "stellwerk-labs/platform-orchestrator-" + name for name in ("cp", "dp", "iam")}
    repository = known.get(module)
    if (not repository or env.get("GITHUB_REPOSITORY") != repository
            or env.get("GITHUB_REF") != "refs/heads/main"
            or env.get("GITHUB_WORKFLOW_REF") != repository + "/.github/workflows/ci.yaml@refs/heads/main"
            or env.get("GITHUB_EVENT_NAME") not in ("push", "workflow_dispatch")):
        raise ValueError("only the original public main release workflow can authenticate artifacts")
    for key in ("RELEASE_SOURCE_SHA", "GITHUB_SHA", "GITHUB_WORKFLOW_SHA"):
        if not SHA.fullmatch(env.get(key, "")):
            raise ValueError("exact source, event and producing workflow commits are required")
    if any(not re.fullmatch(r"[1-9][0-9]*", env.get(key, ""))
           for key in ("GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT")):
        raise ValueError("exact workflow run and attempt are required")
    tag, source = env.get("RELEASE_VERSION", ""), env["RELEASE_SOURCE_SHA"]
    seed = env.get("RELEASE_KIND", "release") == "seed"
    if env.get("RELEASE_KIND", "release") not in ("release", "seed"):
        raise ValueError("unknown publication kind")
    if seed:
        if repository != "stellwerk-labs/platform-orchestrator-dp" or tag != "ci-seed-" + source:
            raise ValueError("only the explicit DP dependency seed is supported")
        if env["GITHUB_EVENT_NAME"] != "workflow_dispatch":
            raise ValueError("seeds require explicit manual dispatch")
    elif not VERSION.fullmatch(tag):
        raise ValueError("canonical release version required")
    elif env["GITHUB_EVENT_NAME"] == "workflow_dispatch" and "-rc." not in tag:
        raise ValueError("manual release must be a candidate")
    elif env["GITHUB_EVENT_NAME"] == "push" and ("-rc." in tag or source != env["GITHUB_SHA"]):
        raise ValueError("stable source must equal the main push commit")
    if env.get("IMAGE_TAG") != "ghcr.io/" + repository + ":" + tag:
        raise ValueError("image destination differs from source repository and approved version")
    if not DIGEST.fullmatch(env.get("IMAGE_DIGEST", "")):
        raise ValueError("exact inspected OCI index required")
    return repository, seed


def provenance(env, repository, seed):
    return {"buildDefinition": {
        "buildType": "https://stellwerk.dev/build/server-image/v1",
        "externalParameters": {"version": env["RELEASE_VERSION"],
                               "purpose": "unqualified-ci-dependency-seed" if seed else "release"},
        "resolvedDependencies": [
            {"uri": "git+https://github.com/" + repository,
             "digest": {"gitCommit": env["RELEASE_SOURCE_SHA"]}},
            {"uri": "git+https://github.com/" + repository + "/.github/workflows/ci.yaml",
             "digest": {"gitCommit": env["GITHUB_WORKFLOW_SHA"]}}]},
        "runDetails": {"builder": {"id": "https://github.com/" + env["GITHUB_WORKFLOW_REF"]},
                       "metadata": {"invocationId": "https://github.com/" + repository + "/actions/runs/"
                                    + env["GITHUB_RUN_ID"] + "/attempts/" + env["GITHUB_RUN_ATTEMPT"]}}}


def evidence(layout, output, env, source=ROOT):
    repository, seed = identity(env, (source / "go.mod").read_text().splitlines()[0])
    report = json.loads((output / "platforms.json").read_bytes())
    if report.get("index") != env["IMAGE_DIGEST"]:
        raise ValueError("inspected index differs from build output")
    if set(report.get("payloads", {})) != ({"amd64"} if seed else {"amd64", "arm64"}):
        raise ValueError("unexpected publication platform set")
    for arch, payload in report["payloads"].items():
        if not DIGEST.fullmatch(payload.get("manifest", "")) or not DIGEST.fullmatch(payload.get("config", "")):
            raise ValueError("invalid inspected platform identity")
        raw = (layout / "blobs/sha256" / payload["config"][7:]).read_bytes()
        if "sha256:" + hashlib.sha256(raw).hexdigest() != payload["config"]:
            raise ValueError("image config changed since inspection")
        labels = json.loads(raw).get("config", {}).get("Labels", {})
        if (labels.get("org.opencontainers.image.source") != "https://github.com/" + repository
                or labels.get("org.opencontainers.image.revision") != env["RELEASE_SOURCE_SHA"]
                or (not seed and labels.get("org.opencontainers.image.version") != env["RELEASE_VERSION"])
                or (seed and labels.get("org.opencontainers.image.description")
                    != "CI dependency seed; not a qualified release")):
            raise ValueError("image labels differ from exact approved source/purpose")
        document = json.loads((output / (arch + ".spdx.json")).read_bytes())
        if (document.get("spdxVersion") != "SPDX-2.3" or document.get("SPDXID") != "SPDXRef-DOCUMENT"
                or document.get("documentNamespace") != "https://stellwerk.dev/sbom/"
                + payload["manifest"].replace(":", "/") + "/linux/" + arch):
            raise ValueError("SBOM differs from inspected platform subject")
    return provenance(env, repository, seed)


def stage(layout, output, env, source=ROOT):
    identity(env, (source / "go.mod").read_text().splitlines()[0])
    if subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip() != env["RELEASE_SOURCE_SHA"]:
        raise ValueError("checkout differs from approved source")
    workflow = subprocess.check_output(["git", "show", env["GITHUB_WORKFLOW_SHA"] + ":.github/workflows/ci.yaml"], cwd=source)
    if workflow != (source / ".github/workflows/ci.yaml").read_bytes():
        raise ValueError("checked-out workflow differs from producing workflow")
    predicate = evidence(layout, output, env, source)
    destination = output / "source-provenance.json"
    if destination.exists() or destination.is_symlink():
        raise ValueError("source provenance already staged")
    destination.write_text(json.dumps(predicate, sort_keys=True, indent=2) + "\n")
    files = sorted(output.iterdir())
    if any(not path.is_file() or path.is_symlink() for path in files):
        raise ValueError("evidence must contain only regular files")
    (output / "SHA256SUMS").write_text("".join(hashlib.sha256(path.read_bytes()).hexdigest()
                                            + "  " + path.name + "\n" for path in files))


def verify(layout, output, env, source=ROOT):
    if json.loads((output / "source-provenance.json").read_bytes()) != evidence(layout, output, env, source):
        raise ValueError("source provenance differs from exact producer")
    records = (output / "SHA256SUMS").read_text().splitlines()
    names = set()
    for record in records:
        match = re.fullmatch(r"([0-9a-f]{64})  ([a-zA-Z0-9_.-]+)", record)
        if not match or match[2] in names:
            raise ValueError("invalid or duplicate evidence checksum record")
        names.add(match[2])
        path = output / match[2]
        if not path.is_file() or path.is_symlink() or hashlib.sha256(path.read_bytes()).hexdigest() != match[1]:
            raise ValueError("staged evidence changed")
    actual = {path.name for path in output.iterdir()}
    if actual != names | {"SHA256SUMS"}:
        raise ValueError("unexpected or missing evidence file")


def statement(path, digest, predicate_type, predicate_path):
    # Only feed this parser output from a successful Cosign verify-attestation.
    if not DIGEST.fullmatch(digest):
        raise ValueError("exact attestation subject digest required")
    raw, values = path.read_text().strip(), []
    decoder = json.JSONDecoder()
    while raw:
        value, end = decoder.raw_decode(raw)
        values.extend(value if isinstance(value, list) else [value])
        raw = raw[end:].lstrip()
    predicate = json.loads(predicate_path.read_bytes())
    for value in values:
        payload = value.get("payload") or value.get("Payload") or value.get("dsseEnvelope", {}).get("payload")
        if payload:
            decoded = json.loads(base64.b64decode(payload, validate=True))
            if (decoded.get("_type") in ("https://in-toto.io/Statement/v0.1", "https://in-toto.io/Statement/v1")
                    and decoded.get("predicateType") == predicate_type and decoded.get("predicate") == predicate
                    and len(decoded.get("subject", [])) == 1
                    and decoded["subject"][0].get("digest") == {"sha256": digest.removeprefix("sha256:")}):
                return
    raise ValueError("verified attestation does not bind the exact subject/type/predicate")


if __name__ == "__main__":
    try:
        command, *args = sys.argv[1:]
        if command == "stage":
            stage(Path(args[0]), Path(args[1]), os.environ)
        elif command == "verify":
            verify(Path(args[0]), Path(args[1]), os.environ)
        elif command == "statement":
            statement(Path(args[0]), args[1], args[2], Path(args[3]))
        else:
            raise ValueError("unknown command")
    except (ValueError, KeyError, OSError, json.JSONDecodeError, subprocess.CalledProcessError):
        raise SystemExit("server publication identity verification failed")
