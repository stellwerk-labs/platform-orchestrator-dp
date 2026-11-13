-- +goose Up
CREATE TABLE deployment_log_material (
    deployment_id UUID PRIMARY KEY REFERENCES deployments(id) ON DELETE CASCADE,
    org_id TEXT NOT NULL,
    recipient TEXT NOT NULL,
    encrypted_identity TEXT NOT NULL CHECK (encrypted_identity ~ '^vault:v[1-9][0-9]*:.+$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE deployment_log_material;
