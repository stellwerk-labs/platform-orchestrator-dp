-- +goose Up
CREATE TABLE retained_plans (
    id UUID PRIMARY KEY,
    org_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    env_id TEXT NOT NULL,
    environment_id UUID NOT NULL,
    creator_id UUID NOT NULL,
    source_deployment_id UUID NOT NULL UNIQUE REFERENCES deployments(id),
    base_deployment_id UUID NOT NULL,
    runner_id TEXT NOT NULL,
    binding JSONB NOT NULL,
    recipient TEXT NOT NULL,
    encrypted_identity TEXT NOT NULL CHECK (encrypted_identity ~ '^vault:v[1-9][0-9]*:.+$'),
    lifecycle JSONB NOT NULL,
    request_digest TEXT NOT NULL,
    ciphertext BYTEA,
    ciphertext_sha256 TEXT,
    subject TEXT,
    approval_subject TEXT,
    apply_deployment_id UUID UNIQUE REFERENCES deployments(id),
    apply_key_digest TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '24 hours'
);

-- +goose Down
DROP TABLE retained_plans;
