-- +goose Up
ALTER TABLE deployments ADD COLUMN module_version_request_digest TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE deployments DROP COLUMN IF EXISTS module_version_request_digest;
