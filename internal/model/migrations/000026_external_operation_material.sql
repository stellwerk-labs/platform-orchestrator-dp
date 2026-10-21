-- +goose Up
CREATE TABLE external_operation_material (
    org_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    operation_ref UUID NOT NULL,
    recipient TEXT NOT NULL,
    encrypted_identity TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, plugin_id, operation_ref)
);

-- +goose Down
DROP TABLE IF EXISTS external_operation_material;
