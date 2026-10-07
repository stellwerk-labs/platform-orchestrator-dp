-- +goose Up
-- Deployment requests that found their Environment busy and asked to wait.
-- A row stores the request intent only; the Deployment is resolved when the
-- row reaches the head of its Environment's queue. There is deliberately no
-- foreign key to deployment_environments: a request may wait for an
-- Environment that Core admitted elsewhere before this Data Plane saw it.
CREATE TABLE environment_deployment_queue (
    id UUID PRIMARY KEY,
    seq BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    org_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    env_id TEXT NOT NULL,
    environment_uuid UUID NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('deploy', 'rollback')),
    intent JSONB NOT NULL,
    intent_sha256 TEXT NOT NULL,
    idempotency_key_digest TEXT,
    expected_base_deployment_id UUID,
    created_by UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'dispatched', 'cancelled', 'expired', 'rejected')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    reason_code TEXT,
    reason_message TEXT,
    settled_by UUID,
    settled_at TIMESTAMPTZ,
    deployment_id UUID UNIQUE,
    CHECK (expires_at > created_at),
    CHECK ((state = 'dispatched') = (deployment_id IS NOT NULL)),
    CHECK ((state = 'queued') = (settled_at IS NULL)),
    CHECK ((state IN ('queued', 'dispatched')) = (reason_code IS NULL))
);

CREATE INDEX environment_deployment_queue_head
    ON environment_deployment_queue (environment_uuid, seq) WHERE state = 'queued';
CREATE INDEX environment_deployment_queue_expiry
    ON environment_deployment_queue (expires_at) WHERE state = 'queued';
CREATE INDEX environment_deployment_queue_listing
    ON environment_deployment_queue (org_id, project_id, env_id, seq);
CREATE INDEX environment_deployment_queue_retention
    ON environment_deployment_queue (settled_at) WHERE state <> 'queued';
CREATE INDEX environment_deployment_queue_idempotency
    ON environment_deployment_queue (org_id, environment_uuid, idempotency_key_digest, seq)
    WHERE idempotency_key_digest IS NOT NULL;
CREATE UNIQUE INDEX environment_deployment_queue_waiting_idempotency
    ON environment_deployment_queue (org_id, environment_uuid, idempotency_key_digest)
    WHERE idempotency_key_digest IS NOT NULL AND state = 'queued';

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM environment_deployment_queue WHERE state = 'queued') THEN
        RAISE EXCEPTION 'cannot discard queued Deployment requests';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE environment_deployment_queue;
