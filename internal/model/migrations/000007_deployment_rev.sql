-- +goose Up

ALTER TABLE deployments ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;

-- +goose Down

ALTER TABLE deployments DROP COLUMN revision;
