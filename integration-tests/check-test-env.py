#!/usr/bin/env python3
"""Check the real generated fixture environment without printing credentials."""
import os
import sys
from urllib.parse import urlparse


def require(condition, name):
    if not condition:
        sys.exit(f"Integration environment is invalid: {name}")


for name in ("DB_CONNECTION_STRING", "CP_DB_CONNECTION_STRING", "IAM_DB_CONNECTION_STRING"):
    fields = os.environ.get(name, "").split()
    require({"host=localhost", "port=" + os.environ["DATABASE_PORT"], "sslmode=disable"} <= set(fields), name)
for name, port in (("SERVER_URL", "PUBLIC_PORT"), ("OIDC_SERVER_URL", "PUBLIC_PORT"),
                   ("INTERNAL_CP_URL", "CP_PORT"), ("INTERNAL_DP_URL", "DP_PORT"),
                   ("INTERNAL_IAM_URL", "IAM_PORT")):
    value = urlparse(os.environ.get(name, ""))
    require(value.scheme == "http" and bool(value.hostname)
            and value.port == int(os.environ[port]), name)
for name in ("RUNNER_TEST_KUBE_CONTEXT", "RUNNER_TOKEN_SALT", "TEST_USER_IDENTITY_RECIPIENT", "NATS_URL"):
    require(bool(os.environ.get(name)), name)
print("Integration environment projection passed")
