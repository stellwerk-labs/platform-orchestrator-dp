#!/usr/bin/env python3
"""publish-oci-layout.sh against a real loopback registry and the pinned ORAS release.

The registry can lag like GHCR did for Data Plane v4.5.0-rc.3 (404 for the first read of a manifest it
has just accepted), lag the same way on the first read of a tag it has just set, or deny every push.
ORAS talks plain HTTP to localhost, so no TLS fixture is needed.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/publish-oci-layout.sh"
UNKNOWN = b'{"errors":[{"code":"MANIFEST_UNKNOWN","message":"manifest unknown"}]}'


class Registry(ThreadingHTTPServer):
    def __init__(self, mode):
        super().__init__(("127.0.0.1", 0), Handler)
        self.mode, self.blobs, self.manifests, self.tags = mode, {}, {}, {}
        self.lagging, self.lagging_tags = set(), set()
        threading.Thread(target=self.serve_forever, daemon=True).start()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def reply(self, code, headers=None, body=b""):
        self.send_response(code)
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def read_body(self):
        length = int(self.headers.get("Content-Length", "0"))
        return self.rfile.read(length) if length else b""

    def route(self):
        registry, path = self.server, self.path.split("?")[0]
        if path == "/v2/":
            return self.reply(200)
        if registry.mode == "deny" and self.command in ("POST", "PUT"):
            self.read_body()
            return self.reply(403, {"Content-Type": "application/json"}, b'{"errors":[{"code":"DENIED","message":"denied"}]}')
        upload = re.fullmatch(r"/v2/(.+)/blobs/uploads/(.*)", path)
        if upload and self.command == "POST":
            self.read_body()
            return self.reply(202, {"Location": f"/v2/{upload[1]}/blobs/uploads/session"})
        if upload and self.command == "PUT":
            data, digest = self.read_body(), re.search(r"digest=([^&]+)", self.path)[1].replace("%3A", ":")
            registry.blobs[digest] = data
            return self.reply(201, {"Docker-Content-Digest": digest})
        match = re.fullmatch(r"/v2/(.+)/(blobs|manifests)/(.+)", path)
        if not match:
            return self.reply(404)
        kind, reference = match[2], match[3]
        if kind == "blobs":
            if reference not in registry.blobs:
                return self.reply(404)
            return self.reply(200, {"Docker-Content-Digest": reference}, registry.blobs[reference])
        if self.command == "PUT":
            data = self.read_body()
            digest = "sha256:" + hashlib.sha256(data).hexdigest()
            registry.manifests[digest] = (self.headers["Content-Type"], data)
            if not reference.startswith("sha256:"):
                registry.tags[reference] = digest
                if registry.mode == "tag-lag":
                    registry.lagging_tags.add(reference)
            if registry.mode == "lag":
                registry.lagging.add(digest)
            return self.reply(201, {"Docker-Content-Digest": digest})
        if reference in registry.lagging_tags:
            registry.lagging_tags.discard(reference)
            return self.reply(404, {"Content-Type": "application/json"}, UNKNOWN)
        digest = registry.tags.get(reference, reference)
        if digest not in registry.manifests or (self.command == "GET" and digest in registry.lagging):
            registry.lagging.discard(digest)
            return self.reply(404, {"Content-Type": "application/json"}, UNKNOWN)
        media, data = registry.manifests[digest]
        return self.reply(200, {"Docker-Content-Digest": digest, "Content-Type": media}, data)

    do_GET = do_HEAD = do_PUT = do_POST = route


def write_layout(root: Path) -> str:
    """A minimal index -> manifest -> config + layer layout tagged v1; returns the index digest."""
    (root / "blobs/sha256").mkdir(parents=True)

    def add(data, media):
        digest = "sha256:" + hashlib.sha256(data).hexdigest()
        (root / "blobs/sha256" / digest[7:]).write_bytes(data)
        return {"mediaType": media, "digest": digest, "size": len(data)}

    def encode(value):
        return json.dumps(value, separators=(",", ":")).encode()

    config = add(b'{"architecture":"amd64","os":"linux"}', "application/vnd.oci.image.config.v1+json")
    layer = add(b"layer", "application/vnd.oci.image.layer.v1.tar+gzip")
    manifest = add(encode({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                           "config": config, "layers": [layer]}), "application/vnd.oci.image.manifest.v1+json")
    manifest["platform"] = {"architecture": "amd64", "os": "linux"}
    index = add(encode({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json",
                        "manifests": [manifest]}), "application/vnd.oci.image.index.v1+json")
    index["annotations"] = {"org.opencontainers.image.ref.name": "v1"}
    (root / "index.json").write_bytes(encode({"schemaVersion": 2, "manifests": [index]}))
    (root / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}')
    return index["digest"]


class PublishOciLayoutTest(unittest.TestCase):
    def publish(self, mode, expected=None, source_tag="v1"):
        registry = Registry(mode)
        self.addCleanup(registry.server_close)
        self.addCleanup(registry.shutdown)
        with tempfile.TemporaryDirectory() as directory:
            root = write_layout(Path(directory) / "layout")
            tag = f"localhost:{registry.server_address[1]}/stellwerk/image:v1"
            result = subprocess.run(["bash", str(SCRIPT), f"{directory}/layout:{source_tag}", tag, expected or root],
                                    env=dict(os.environ, PUBLISH_RETRY_PAUSE_SECONDS="0"),
                                    capture_output=True, text=True, timeout=120)
        return registry, root, result

    def test_repeats_the_copy_while_the_registry_lags(self):
        registry, root, result = self.publish("lag")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(registry.tags["v1"], root)
        self.assertIn("copy attempt 1 of 6, retrying", result.stderr)

    def test_readback_waits_while_the_new_tag_lags(self):
        registry, root, result = self.publish("tag-lag")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(registry.tags["v1"], root)
        self.assertEqual(registry.lagging_tags, set(), "the readback never met the lagging tag")
        self.assertNotIn("retrying", result.stderr)

    def test_wrong_expected_digest_fails_after_one_copy(self):
        _, _, result = self.publish("normal", expected="sha256:" + "0" * 64)
        self.assertEqual(result.returncode, 1)
        self.assertIn("resolves to", result.stderr)
        self.assertNotIn("retrying", result.stderr)

    def test_denied_push_is_not_repeated(self):
        registry, _, result = self.publish("deny")
        self.assertNotEqual(result.returncode, 0)
        self.assertNotIn("retrying", result.stderr)
        self.assertEqual(registry.tags, {})

    def test_missing_layout_tag_is_not_mistaken_for_registry_lag(self):
        # ORAS also ends this deterministic source error with "not found"; only the registry form retries.
        registry, _, result = self.publish("normal", source_tag="v2")
        self.assertEqual(result.returncode, 1)
        self.assertNotIn("retrying", result.stderr)
        self.assertEqual(registry.tags, {})


if __name__ == "__main__":
    unittest.main()
