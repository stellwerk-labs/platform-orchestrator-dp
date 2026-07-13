-- +goose Up

ALTER TABLE deployments ADD COLUMN created_by UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000';

-- +goose Down

ALTER TABLE deployments DROP COLUMN created_by;
