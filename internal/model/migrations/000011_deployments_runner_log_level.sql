-- +goose Up

ALTER TABLE deployments ADD COLUMN IF NOT EXISTS runner_log_level TEXT CHECK (runner_log_level IN ('debug', 'info', 'warn', 'error')) NOT NULL DEFAULT 'info';

-- +goose Down

ALTER TABLE deployments DROP COLUMN IF EXISTS runner_log_level;