-- +goose Up

CREATE INDEX res_node_module_idx ON resource_nodes (env_uuid, last_module_definition_id, last_module_definition_version);

-- +goose Down

DROP INDEX res_node_module_idx;
