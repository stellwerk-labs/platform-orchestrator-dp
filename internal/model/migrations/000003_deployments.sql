-- +goose Up

-- deployment_environments exists to hold the latest deployment for an environment as well as it's state uuid. Both
-- of these are critical to support redeployment and iteration on an environment: (1) the latest deployment columns makes
-- it very efficient to get the latest deployment state for an environment, a very common operation; and (2) the state
-- uuid ensures that the next deployment continues using the same backend TF state key that the previous deployment used.
-- without this, there's no way to ensure that different iterations of an environment have distinct state files.
CREATE TABLE deployment_environments (
    id UUID PRIMARY KEY NOT NULL,

    -- currently, our deployment environments are tied to logical environments, but they _could_ be coopted with
    -- different identifiers for other more temporary environments.
    org_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    env_id TEXT NOT NULL,

    -- we want constant time lookup of the latest deployment per environment without scanning through all deployments
    -- however sometimes we also need to know the last non-plan deployment which may have changed the graph.
    last_executing_deployment_id UUID NOT NULL,
    last_executing_deployment_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    last_state_deployment_id UUID NOT NULL,
    last_state_deployment_at TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

-- must have a unique logical environment
CREATE UNIQUE INDEX ON deployment_environments (org_id, project_id, env_id);

-- deployments are the actual logical deployment in an environment
CREATE TABLE deployments (
    id UUID PRIMARY KEY NOT NULL,
    de_id UUID NOT NULL REFERENCES deployment_environments (id) ON DELETE CASCADE,

    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('plan_only', 'deploy', 'destroy')),
    completed_at TIMESTAMP WITHOUT TIME ZONE,
    status TEXT NOT NULL CHECK (status IN ('executing', 'failed', 'succeeded')),
    status_message TEXT NOT NULL,

    -- optional idempotency key associated with this deployment
    idempotency_key_digest TEXT,

    -- We use bytea here because it's more efficient for insert/extract when we don't need any particular structured
    -- queries.
    manifest BYTEA NOT NULL,
    graph BYTEA NOT NULL,
    tofu BYTEA NOT NULL,

    -- Runner id must be stored per deployment, because this can change over time, and we must record which runner was
    -- used.
    runner_id TEXT NOT NULL,

    -- Public key and encrypted outputs, eventually sent by the runner
    encrypted_outputs_recipient TEXT,
    outputs BYTEA,

    -- Object containing terraform resource metrics
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- We do need an extra index on created at, so that we can return an ordered history over the org, or env when filtered.
CREATE INDEX ON deployments (created_at);
-- We have to add this constraint after the table definition to avoid ordering cycle issues.
ALTER TABLE deployment_environments ADD CONSTRAINT de_exec_dep_fk FOREIGN KEY (last_executing_deployment_id) REFERENCES deployments (id) ON DELETE RESTRICT INITIALLY DEFERRED;
ALTER TABLE deployment_environments ADD CONSTRAINT de_state_dep_fk FOREIGN KEY (last_state_deployment_id) REFERENCES deployments (id) ON DELETE RESTRICT INITIALLY DEFERRED;

-- +goose Down

ALTER TABLE deployment_environments DROP CONSTRAINT de_exec_dep_fk;
ALTER TABLE deployment_environments DROP CONSTRAINT de_state_dep_fk;
DROP TABLE IF EXISTS deployments;
DROP TABLE IF EXISTS deployment_environments;
