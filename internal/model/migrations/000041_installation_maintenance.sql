-- +goose Up
CREATE TABLE installation_maintenance (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    enabled BOOLEAN NOT NULL DEFAULT false,
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_by UUID
);
INSERT INTO installation_maintenance(singleton) VALUES(true);
CREATE TABLE installation_maintenance_events (
    revision BIGINT PRIMARY KEY,
    enabled BOOLEAN NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    changed_by UUID NOT NULL
);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM installation_maintenance WHERE enabled) THEN
        RAISE EXCEPTION 'disable installation maintenance before removing its admission barrier';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE installation_maintenance_events;
DROP TABLE installation_maintenance;
