-- +goose Up
CREATE TABLE external_operation_deployments (
    org_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    operation_ref TEXT NOT NULL,
    fence BIGINT NOT NULL CHECK (fence > 0),
    request_id TEXT NOT NULL,
    assertion_action TEXT NOT NULL,
    service_action TEXT NOT NULL,
    request_body_sha256 TEXT NOT NULL CHECK (request_body_sha256 ~ '^[0-9a-f]{64}$'),
    actor_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    env_id TEXT NOT NULL,
    deployment_id UUID NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, plugin_id, request_id)
);

-- The deployment deliberately has no foreign key. Operation receipts outlive
-- normal deployment retention and prevent a deleted deployment from making the
-- same external request executable again.
CREATE INDEX external_operation_deployments_operation
    ON external_operation_deployments (org_id, plugin_id, operation_ref);

-- +goose Down
DROP TABLE IF EXISTS external_operation_deployments;
