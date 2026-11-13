-- +goose Up
CREATE TABLE installation_cost_policy (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    enabled BOOLEAN NOT NULL DEFAULT false,
    api_key_ciphertext TEXT NOT NULL DEFAULT '',
    revision BIGINT NOT NULL DEFAULT 1 CHECK (revision > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    updated_by UUID,
    CHECK (NOT enabled OR api_key_ciphertext <> '')
);
INSERT INTO installation_cost_policy(singleton) VALUES(true);
CREATE TABLE installation_cost_policy_events (
    revision BIGINT PRIMARY KEY,
    enabled BOOLEAN NOT NULL,
    api_key_configured BOOLEAN NOT NULL,
    api_key_changed BOOLEAN NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT statement_timestamp(),
    changed_by UUID NOT NULL
);
CREATE TABLE deployment_cost_policy_snapshots (
    deployment_id UUID PRIMARY KEY REFERENCES deployments(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL,
    policy_revision BIGINT NOT NULL CHECK (policy_revision > 0)
);

-- +goose Down
DROP TABLE deployment_cost_policy_snapshots;
DROP TABLE installation_cost_policy_events;
DROP TABLE installation_cost_policy;
