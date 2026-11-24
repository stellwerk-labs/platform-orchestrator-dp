-- +goose Up
-- Like admission receipts, control authority deliberately outlives Deployment
-- retention. In particular, undelivered controls must never disappear by cascade.
CREATE TABLE deployment_pipeline_controls (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL,
    deployment_id UUID NOT NULL,
    environment_id UUID NOT NULL,
    actor_id UUID NOT NULL,
    client_key TEXT NOT NULL CHECK (octet_length(client_key) BETWEEN 1 AND 128),
    intent_digest TEXT NOT NULL CHECK (intent_digest ~ '^[0-9a-f]{64}$'),
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    execution_digest TEXT NOT NULL CHECK (execution_digest ~ '^[0-9a-f]{64}$'),
    runner_uuid UUID NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('approve','deny','cancel','repair')),
    command BYTEA NOT NULL CHECK (octet_length(command) BETWEEN 1 AND 8192),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL CHECK (expires_at > created_at),
    result BYTEA,
    result_event_digest TEXT CHECK (result_event_digest ~ '^[0-9a-f]{64}$'),
    resolved_at TIMESTAMPTZ,
    UNIQUE (org_id, deployment_id, actor_id, client_key),
    CHECK ((result IS NULL AND result_event_digest IS NULL AND resolved_at IS NULL) OR
           (result IS NOT NULL AND result_event_digest IS NOT NULL AND resolved_at IS NOT NULL))
);
CREATE INDEX deployment_pipeline_controls_deployment ON deployment_pipeline_controls(org_id,deployment_id);
-- No automatic GC: deletion needs an explicit replay-authority/delivery policy.

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployment_pipeline_controls) THEN
        RAISE EXCEPTION 'cannot discard retained Native control authority';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE deployment_pipeline_controls;
