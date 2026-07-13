-- +goose Up

CREATE TABLE IF NOT EXISTS resource_nodes (
    -- The hash should be sha256(de_id resource_type resource_class resource_id) and serves as a deterministic
    -- primary key for use in the adjacency matrix table.
    hash TEXT NOT NULL PRIMARY KEY,

    -- The environment uuid maps to the deployment_environments table to pick up org, project, env, and deployment id.
    env_uuid UUID NOT NULL REFERENCES deployment_environments (id) ON DELETE CASCADE,
    resource_type TEXT NOT NULL,
    resource_class TEXT NOT NULL,
    resource_id TEXT NOT NULL,

    -- We need to store the last deployment id so that we can tell which nodes or edges are up to date and which ones
    -- need to be removed at the end of a deployment or need to be marked as deleting.
    last_deployment_id UUID NOT NULL,

    -- This is necessary so that we can map easily into the definitions table and show info about how and where the
    -- node was provisioned.
    last_module_definition_id TEXT NOT NULL,
    last_module_definition_version TEXT NOT NULL,

    -- Metadata produced by platform_orchestrator_metadata output
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX ON resource_nodes (env_uuid, resource_type, resource_class, resource_id);

CREATE TABLE IF NOT EXISTS resource_nodes_depends_on (
    -- key mapping to the resource nodes
    subject_hash TEXT NOT NULL REFERENCES resource_nodes(hash) ON DELETE CASCADE,
    target_hash TEXT NOT NULL REFERENCES resource_nodes(hash) ON DELETE CASCADE,
    -- We need to store the last deployment id so that we can tell if this edge is present in the target state
    -- or is still being removed in this deployment.
    last_deployment_id UUID NOT NULL,
    -- label to add to the edge
    edge_alias TEXT NOT NULL,
    PRIMARY KEY (subject_hash, target_hash)
);

-- +goose Down

DROP TABLE IF EXISTS resource_nodes_depends_on;
DROP TABLE IF EXISTS resource_nodes;
