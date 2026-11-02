-- +goose Up
ALTER TABLE deployments DROP CONSTRAINT deployments_mode_check;
ALTER TABLE deployments ADD CONSTRAINT deployments_mode_check CHECK (mode IN ('plan_only','deploy','rollback','rollback_plan','destroy','observation'));
CREATE TABLE runner_job_reservations (
    deployment_id UUID PRIMARY KEY REFERENCES deployments(id) ON DELETE RESTRICT,
    org_id TEXT NOT NULL,
    runner_id TEXT NOT NULL,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at TIMESTAMPTZ
);
CREATE INDEX runner_job_reservations_active ON runner_job_reservations(org_id,runner_id) WHERE released_at IS NULL;
CREATE INDEX runner_job_reservations_live_deployment ON runner_job_reservations(deployment_id) WHERE released_at IS NULL;
CREATE INDEX deployments_running_environment ON deployments(de_id) WHERE status='executing';
-- Existing attempts have no slot record. Preserve all potentially live ownership,
-- including failed attempts whose watchdog did not prove that the Runner stopped.
-- Legacy success was written by authenticated completion even before markers
-- existed; it is positive stop evidence and must not strand capacity on opt-in.
INSERT INTO runner_job_reservations(deployment_id,org_id,runner_id,reserved_at)
SELECT d.id,e.org_id,d.runner_id,COALESCE(b.first_attempt_at,d.created_at AT TIME ZONE 'UTC')
FROM deployments d JOIN deployment_environments e ON e.id=d.de_id
LEFT JOIN deployment_dispatch_budgets b ON b.deployment_id=d.id
WHERE (d.status='executing' OR (d.status='failed' AND (b.deployment_id IS NULL OR b.first_attempt_at IS NOT NULL) AND b.completion_received_at IS NULL))
AND NOT EXISTS(SELECT 1 FROM runner_event_inbox i WHERE i.deployment_id=d.id AND i.org_id=e.org_id AND i.runner_id=d.runner_id AND i.event_type='deployment-result')
AND NOT EXISTS(SELECT 1 FROM deployment_late_runner_results l WHERE l.deployment_id=d.id)
AND NOT EXISTS(SELECT 1 FROM deployment_outcome_acknowledgements a WHERE a.deployment_id=d.id);
CREATE TABLE runner_job_completion_receipts (
    deployment_id UUID PRIMARY KEY REFERENCES deployments(id) ON DELETE CASCADE,
    org_id TEXT NOT NULL,
    runner_id TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK(payload_sha256 ~ '^[a-f0-9]{64}$'),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE environment_observation_receipts (
    org_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    request_id UUID NOT NULL,
    request_sha256 TEXT NOT NULL CHECK(request_sha256 ~ '^[a-f0-9]{64}$'),
    environment_uuid UUID NOT NULL,
    actor_id UUID NOT NULL,
    grant_id UUID NOT NULL,
    grant_revision BIGINT NOT NULL CHECK(grant_revision>0),
    receipt JSONB NOT NULL,
    deployment_id UUID UNIQUE REFERENCES deployments(id) ON DELETE SET NULL,
    baseline_id UUID REFERENCES deployments(id) ON DELETE SET NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completion_sha256 TEXT CHECK(completion_sha256 IS NULL OR completion_sha256 ~ '^[a-f0-9]{64}$'),
    result JSONB CHECK(result IS NULL OR octet_length(result::text)<=1048576),
    result_expires_at TIMESTAMPTZ,
    PRIMARY KEY(org_id,plugin_id,request_id)
);
CREATE INDEX environment_observation_active ON environment_observation_receipts(environment_uuid) WHERE deployment_id IS NOT NULL;
CREATE UNIQUE INDEX environment_observation_run_identity ON environment_observation_receipts(org_id,plugin_id,(receipt->>'run_id')) WHERE receipt ? 'run_id';
CREATE INDEX environment_observation_result_retention ON environment_observation_receipts(result_expires_at) WHERE result IS NOT NULL;
CREATE TABLE environment_observation_redeploy_receipts (
    org_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    request_id UUID NOT NULL,
    run_id UUID NOT NULL,
    actor_id UUID NOT NULL,
    request_sha256 TEXT NOT NULL CHECK(request_sha256 ~ '^[a-f0-9]{64}$'),
    deployment_id UUID NOT NULL,
    baseline_id UUID NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(org_id,plugin_id,request_id)
);
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
  IF EXISTS(SELECT 1 FROM environment_observation_receipts) OR EXISTS(SELECT 1 FROM environment_observation_redeploy_receipts) OR EXISTS(SELECT 1 FROM runner_job_reservations WHERE released_at IS NULL) OR EXISTS(SELECT 1 FROM runner_job_completion_receipts) THEN
    RAISE EXCEPTION 'cannot discard observation receipts or live Runner ownership; use a compatible forward recovery';
  END IF;
END $$;
-- +goose StatementEnd
DROP TABLE environment_observation_redeploy_receipts;
DROP TABLE environment_observation_receipts;
DROP TABLE runner_job_completion_receipts;
DROP TABLE runner_job_reservations;
DROP INDEX deployments_running_environment;
ALTER TABLE deployments DROP CONSTRAINT deployments_mode_check;
ALTER TABLE deployments ADD CONSTRAINT deployments_mode_check CHECK (mode IN ('plan_only','deploy','rollback','rollback_plan','destroy'));
