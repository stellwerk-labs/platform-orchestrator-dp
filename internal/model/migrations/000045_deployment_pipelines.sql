-- +goose Up
CREATE TABLE deployment_pipelines (
    id uuid PRIMARY KEY,
    org_id text NOT NULL,
    project_id text NOT NULL,
    env_id text NOT NULL,
    environment_uuid uuid NOT NULL,
    actor_id uuid NOT NULL,
    spec jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX deployment_pipelines_environment ON deployment_pipelines(org_id, project_id, env_id, created_at DESC);
CREATE TABLE deployment_pipeline_steps (
    pipeline_id uuid NOT NULL REFERENCES deployment_pipelines(id),
    step_id text NOT NULL,
    result jsonb,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY(pipeline_id, step_id)
);
CREATE TABLE deployment_pipeline_decisions (
    id uuid PRIMARY KEY,
    pipeline_id uuid NOT NULL REFERENCES deployment_pipelines(id),
    actor_id uuid NOT NULL,
    subject text NOT NULL,
    action text NOT NULL CHECK(action IN ('approve','repair')),
    step_id text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- +goose Down
DROP TABLE deployment_pipeline_decisions;
DROP TABLE deployment_pipeline_steps;
DROP TABLE deployment_pipelines;
