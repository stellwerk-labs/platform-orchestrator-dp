-- +goose Up
CREATE TABLE deployment_dispatch_budgets (
    deployment_id UUID PRIMARY KEY REFERENCES deployments(id) ON DELETE CASCADE,
    deployment_revision INTEGER NOT NULL,
    first_attempt_at TIMESTAMPTZ,
    deadline TIMESTAMPTZ,
    completion_received_at TIMESTAMPTZ,
    legacy BOOLEAN NOT NULL DEFAULT false,
    CHECK ((first_attempt_at IS NULL) = (deadline IS NULL)),
    CHECK (deadline IS NULL OR deadline > first_attempt_at)
);
-- Previous versions have no durable first-dispatch marker. Never refresh an
-- unknown legacy attempt's budget on upgrade or broker replay.
INSERT INTO deployment_dispatch_budgets (deployment_id, deployment_revision, first_attempt_at, deadline, legacy)
SELECT id, CASE WHEN completed_at IS NULL THEN revision ELSE greatest(1, revision - 1) END,
       created_at AT TIME ZONE 'UTC', (created_at AT TIME ZONE 'UTC') + interval '1 hour', true FROM deployments;

CREATE TABLE deployment_late_runner_results (
    deployment_id UUID PRIMARY KEY REFERENCES deployments(id) ON DELETE CASCADE,
    event_id TEXT NOT NULL UNIQUE,
    org_id TEXT NOT NULL,
    runner_id TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
    result_status TEXT NOT NULL CHECK (result_status IN ('success', 'failure')),
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    encrypted_outputs BYTEA,
    recipient TEXT,
    CHECK (encrypted_outputs IS NULL OR recipient IS NOT NULL)
);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployment_late_runner_results) OR
       EXISTS (SELECT 1 FROM deployment_dispatch_budgets WHERE NOT legacy AND first_attempt_at IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot discard durable Runner dispatch or late completion evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE deployment_late_runner_results;
DROP TABLE deployment_dispatch_budgets;
