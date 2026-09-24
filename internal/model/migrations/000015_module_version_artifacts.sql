-- +goose Up
ALTER TABLE deployments ADD COLUMN module_artifacts JSONB NOT NULL DEFAULT '[]'::jsonb;

-- +goose Down
ALTER TABLE deployments DROP COLUMN IF EXISTS module_artifacts;
