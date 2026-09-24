#!/usr/bin/env bash
# Source from this directory after generating compose.yaml. Values stay in the
# process environment; do not run with shell tracing enabled.
export SERVER_URL="http://$(score-compose resources get-outputs 'dns.default#api-dns' -f '{{ .host }}'):${PUBLIC_PORT:-8080}"
export OIDC_SERVER_URL="http://$(score-compose resources get-outputs 'dns.default#platform-orchestrator-dp.dns-oidc' -f '{{ .host }}'):${PUBLIC_PORT:-8080}"
export INTERNAL_CP_URL="http://localhost:${CP_PORT:-8081}"
export INTERNAL_DP_URL="http://localhost:${DP_PORT:-8082}"
export INTERNAL_IAM_URL="http://localhost:${IAM_PORT:-8083}"
export DB_CONNECTION_STRING="$(score-compose resources get-outputs 'postgres.default#platform-orchestrator-dp.postgres' -f "host=localhost port=${DATABASE_PORT:-5432} dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable")"
export CP_DB_CONNECTION_STRING="$(score-compose resources get-outputs 'postgres.default#platform-orchestrator-cp.postgres' -f "host=localhost port=${DATABASE_PORT:-5432} dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable")"
export IAM_DB_CONNECTION_STRING="$(score-compose resources get-outputs 'postgres.default#platform-orchestrator-iam.postgres' -f "host=localhost port=${DATABASE_PORT:-5432} dbname={{ .name }} user={{ .username }} password={{ .password }} connect_timeout=5 sslmode=disable")"
export NATS_URL="nats://localhost:${NATS_PORT:-4222}"
export RUNNER_TOKEN_SALT='vnXd0Ses8L1X0TGGcp+mf/V8dQs+L4/fWvijORwb7lI='
export TEST_USER_IDENTITY_RECIPIENT='age10hmkvsqfxr5ha05ay6pqj9zdtal7pw7x2r3t4n00rzucc3en3u9q3cycqm'
