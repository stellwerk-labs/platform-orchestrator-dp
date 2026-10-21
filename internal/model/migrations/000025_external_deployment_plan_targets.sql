-- +goose Up
ALTER TABLE external_deployment_plan_snapshots
    ADD COLUMN explicit_targets JSONB NOT NULL DEFAULT '[]'::jsonb;

-- +goose Down
ALTER TABLE external_deployment_plan_snapshots DROP COLUMN explicit_targets;
