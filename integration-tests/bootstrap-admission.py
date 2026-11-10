#!/usr/bin/env python3
"""Configure the disposable Compose stack's real IAM-authenticated Core route."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request


def request(url, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"From": "ffffffff-ffff-ffff-ffff-ffffffffffff",
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def token_is_valid(iam, principal):
    req = urllib.request.Request(iam + "/internal/authenticate", method="POST",
                                 headers={"Authorization": "Bearer " + principal["token"]})
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return response.headers.get("From") == principal["id"]
    except urllib.error.HTTPError as error:
        if error.code == 401:
            return False
        raise


def wait_ready(port, path="/health", headers=None):
    deadline = time.monotonic() + 60
    while True:
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", headers=headers or {})
            with urllib.request.urlopen(req, timeout=2) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError(f"service on port {port} did not become healthy")
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--iam-port", type=int, default=8083)
    args = parser.parse_args()
    os.umask(0o077)
    root = Path(".score-compose/admission")
    root.mkdir(parents=True, exist_ok=True)
    iam = f"http://127.0.0.1:{args.iam_port}"
    credential = root / "principal.json"
    principal = None
    if credential.exists():
        saved = json.loads(credential.read_text())
        try:
            request(iam + "/platform-principals/" + saved["id"])
            if token_is_valid(iam, saved):
                principal = saved
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
    if principal is None:
        principal = request(iam + "/platform-principals", "POST", {
            "display_name": "Disposable integration Data Plane", "expiry_in_days": 1,
        })
        credential.write_text(json.dumps(principal))
    token = root / "token"
    token.write_text(principal["token"])
    # The containing directory is private; the nonroot DP must read the mount.
    token.chmod(0o644)
    nginx = root / "nginx.conf"
    nginx.write_text("""events {}
http {
  resolver 127.0.0.11 valid=1s;
  server {
    listen 8080;
    location = /_authenticate {
      internal;
      set $iam http://platform-orchestrator-iam-platform-orchestrator-iam:8080;
      proxy_pass $iam/internal/authenticate;
      proxy_method POST;
      proxy_pass_request_body off;
      proxy_set_header Content-Length "";
      proxy_set_header Authorization $http_authorization;
      proxy_set_header From "";
    }
    location / {
      auth_request /_authenticate;
      auth_request_set $principal $upstream_http_from;
      proxy_set_header From $principal;
      set $core http://platform-orchestrator-cp-platform-orchestrator-cp:8080;
      proxy_pass $core$request_uri;
    }
  }
}
""")
    config = json.loads(subprocess.check_output(["docker", "compose", "config", "--format", "json"]))
    routers = [(name, service) for name, service in config["services"].items()
               if "nginx" in service.get("image", "") and service.get("ports")]
    if len(routers) != 1:
        raise RuntimeError("exactly one public Score router is required")
    router_name, router = routers[0]
    override = {"services": {
        "platform-orchestrator-cp-platform-orchestrator-cp": {
            "environment": {"DEPLOYMENT_ADMISSION_IDENTITY": principal["id"]}},
        "platform-orchestrator-dp-platform-orchestrator-dp": {
            "environment": {
                "DEPLOYMENT_ADMISSION_CORE_GATEWAY_URL": "http://admission-gateway:8080",
                "DEPLOYMENT_ADMISSION_CORE_BEARER_TOKEN_FILE": "/run/admission/token"},
            "volumes": [{"type": "bind", "source": str(token.resolve()),
                         "target": "/run/admission/token", "read_only": True}]},
        "admission-gateway": {
            "image": router["image"],
            "volumes": [{"type": "bind", "source": str(nginx.resolve()),
                         "target": "/etc/nginx/nginx.conf", "read_only": True}],
            "depends_on": ["platform-orchestrator-cp-platform-orchestrator-cp",
                           "platform-orchestrator-iam-platform-orchestrator-iam"]},
    }}
    override_file = root / "compose.json"
    override_file.write_text(json.dumps(override))
    subprocess.run(["docker", "compose", "-f", "compose.yaml", "-f", str(override_file),
                    "up", "--no-build", "-d", "--wait", "admission-gateway",
                    "platform-orchestrator-dp-platform-orchestrator-dp"], check=True)
    for name in ["platform-orchestrator-cp-platform-orchestrator-cp",
                 "platform-orchestrator-dp-platform-orchestrator-dp"]:
        wait_ready(config["services"][name]["ports"][0]["published"])
    # The generated proxy resolves upstream addresses when its configuration loads.
    subprocess.run(["docker", "compose", "-f", "compose.yaml", "-f", str(override_file),
                    "exec", "-T", router_name, "nginx", "-s", "reload"], check=True)
    host = subprocess.check_output(["score-compose", "resources", "get-outputs",
                                    "dns.default#api-dns", "-f", "{{ .host }}"], text=True).strip()
    wait_ready(router["ports"][0]["published"], "/orgs/routing-readiness-probe/projects",
               {"Host": host, "From": "ffffffff-ffff-ffff-ffff-ffffffffffff"})
    print("Configured real IAM platform-principal admission and material catalogue")


if __name__ == "__main__":
    main()
