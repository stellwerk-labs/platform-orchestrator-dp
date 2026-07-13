-- +goose Up
-- Public key to encrypt logs, eventually sent by the runner
ALTER TABLE deployments ADD COLUMN IF NOT EXISTS encrypted_logs_recipient TEXT;
-- +goose Down

ALTER TABLE deployments DROP COLUMN IF EXISTS encrypted_logs_recipient;