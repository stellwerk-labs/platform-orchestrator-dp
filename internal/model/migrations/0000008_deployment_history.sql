-- +goose Up

CREATE TABLE deployment_history (
    id UUID PRIMARY KEY NOT NULL,
    org_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    env_id TEXT NOT NULL,
    env_uuid UUID NOT NULL,
    mode TEXT NOT NULL,
    created_by UUID NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    status TEXT NOT NULL,
    metrics JSONB NOT NULL
);

CREATE INDEX ON deployment_history(org_id, project_id, env_id);
CREATE INDEX ON deployment_history(created_at);
CREATE INDEX ON deployment_history(created_by);

-- +goose Down

DROP TABLE IF EXISTS deployment_history;
