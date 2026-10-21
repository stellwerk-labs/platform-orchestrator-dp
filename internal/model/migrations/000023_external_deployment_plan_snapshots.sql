-- +goose Up
CREATE TABLE external_deployment_plan_snapshots (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    actor_id UUID NOT NULL,
    request_body_sha256 TEXT NOT NULL CHECK (request_body_sha256 ~ '^[a-f0-9]{64}$'),
    project_id TEXT NOT NULL,
    project_uuid UUID NOT NULL,
    env_id TEXT NOT NULL,
    environment_uuid UUID NOT NULL,
    base_deployment_id UUID NOT NULL,
    fingerprint TEXT NOT NULL CHECK (fingerprint ~ '^sha256:[a-f0-9]{64}$'),
    manifest JSONB NOT NULL,
    graph JSONB NOT NULL,
    diff JSONB NOT NULL,
    module_artifacts JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_id, plugin_id, request_id)
);

CREATE INDEX external_deployment_plan_snapshots_environment
    ON external_deployment_plan_snapshots (org_id, project_id, env_id, created_at DESC);

-- +goose Down
DROP TABLE external_deployment_plan_snapshots;
