-- +goose Up
ALTER TABLE deployment_pipeline_runs
    ADD COLUMN effect_base_deployment_id UUID REFERENCES deployments(id) DEFERRABLE INITIALLY DEFERRED,
    ADD COLUMN effect_base_captured BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN no_effect_blocked BOOLEAN NOT NULL DEFAULT false;

UPDATE deployment_pipeline_runs SET no_effect_blocked=true
WHERE progress->>'infrastructure_applied'='true' OR progress->>'phase'='outcome_unknown';

CREATE TABLE pipeline_no_effect_receipts (
    deployment_id UUID PRIMARY KEY REFERENCES deployment_pipeline_runs(deployment_id) ON DELETE CASCADE,
    command_sha256 TEXT NOT NULL CHECK (command_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE VIEW deployment_effective_state AS
SELECT environment.id AS environment_id,
       CASE WHEN receipt.deployment_id IS NOT NULL AND pipeline.effect_base_captured
            THEN pipeline.effect_base_deployment_id
            ELSE environment.last_state_deployment_id END AS deployment_id
FROM deployment_environments environment
LEFT JOIN deployment_pipeline_runs pipeline ON pipeline.deployment_id=environment.last_state_deployment_id
LEFT JOIN pipeline_no_effect_receipts receipt ON receipt.deployment_id=pipeline.deployment_id;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployment_pipeline_runs WHERE effect_base_captured OR no_effect_blocked)
       OR EXISTS (SELECT 1 FROM pipeline_no_effect_receipts) THEN
        RAISE EXCEPTION 'cannot discard captured pipeline base or no-effect evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP VIEW deployment_effective_state;
DROP TABLE pipeline_no_effect_receipts;
ALTER TABLE deployment_pipeline_runs DROP COLUMN no_effect_blocked, DROP COLUMN effect_base_captured, DROP COLUMN effect_base_deployment_id;
