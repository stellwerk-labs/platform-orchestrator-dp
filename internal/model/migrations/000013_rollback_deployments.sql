-- +goose Up

ALTER TABLE deployments
    ADD COLUMN rollback_to_id UUID NULL,
    DROP CONSTRAINT IF EXISTS deployments_mode_check,
    ADD CHECK (mode IN ('plan_only', 'deploy', 'rollback', 'rollback_plan', 'destroy'));

-- +goose Down

ALTER TABLE deployments
    DROP COLUMN rollback_to_id,
    DROP CONSTRAINT IF EXISTS deployments_mode_check,
    ADD CHECK (mode IN ('plan_only', 'deploy', 'destroy'));
