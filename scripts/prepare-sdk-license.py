#!/usr/bin/env python3
"""Attach the declared root license to a verified, disposable SDK snapshot."""
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tarfile


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args])


def prepare(repo, sha, subdirectory, destination):
    repo, destination = Path(repo).resolve(), Path(destination).resolve()
    if repo == destination or repo in destination.parents or not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("SDK destination must be outside source repository at an exact SHA")
    if git(repo, "rev-parse", sha + "^{commit}").decode().strip() != sha:
        raise ValueError("source commit mismatch")
    tree = git(repo, "rev-parse", sha + ":" + subdirectory).decode().strip()
    source = git(repo, "archive", "--format=tar", sha + ":" + subdirectory)
    expected = {}
    with tarfile.open(fileobj=io.BytesIO(source), mode="r:") as archive:
        for entry in archive:
            name = Path(entry.name)
            if name.is_absolute() or ".." in name.parts or entry.issym() or entry.islnk():
                raise ValueError("unsafe SDK archive")
            if entry.isfile():
                expected[entry.name] = archive.extractfile(entry).read()
            elif not entry.isdir():
                raise ValueError("unsupported SDK archive entry")
    actual = {}
    for path in destination.rglob("*"):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError("SDK snapshot must not contain links or special files")
        if path.is_file():
            actual[path.relative_to(destination).as_posix()] = path.read_bytes()
    if actual != expected:
        raise ValueError("SDK snapshot does not match exact source tree")
    license_bytes = git(repo, "show", sha + ":LICENSE")
    license_blob = git(repo, "rev-parse", sha + ":LICENSE").decode().strip()
    module = re.search(rb"(?m)^module (\S+)\s*$", expected["go.mod"]).group(1).decode()
    policy = Path(__file__).with_name("sdk-licenses.tsv").read_text().splitlines()
    approved = [line.split("|") for line in policy if line and not line.startswith("#") and line.split("|")[0] == module]
    if len(approved) != 1:
        raise ValueError("SDK has no exact declared license policy")
    _, spdx, checksum, approved_blob = approved[0]
    if hashlib.sha256(license_bytes).hexdigest() != checksum or license_blob != approved_blob:
        raise ValueError("source SDK license differs from reviewed declaration")
    if "LICENSE" in expected and expected["LICENSE"] != license_bytes:
        raise ValueError("SDK module license conflicts with declared repository license")
    receipt = {"module": module, "sourceCommit": sha, "sourceTree": tree, "licenseBlob": license_blob,
               "licenseSHA256": checksum, "spdx": spdx}
    (destination / "LICENSE").write_bytes(license_bytes)
    (destination / "LICENSE.provenance").write_text(json.dumps(receipt, sort_keys=True) + "\n")
    return receipt


if __name__ == "__main__":
    try:
        print(json.dumps(prepare(*sys.argv[1:]), sort_keys=True))
    except (ValueError, OSError, KeyError, AttributeError, subprocess.CalledProcessError, tarfile.TarError):
        print("SDK license provenance validation failed", file=sys.stderr)
        sys.exit(1)
