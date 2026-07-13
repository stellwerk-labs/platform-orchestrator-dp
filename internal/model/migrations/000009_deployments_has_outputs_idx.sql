-- +goose Up

CREATE INDEX deployments_with_outputs ON deployments (id) WHERE outputs != ''::bytea;

-- +goose Down

DROP INDEX IF EXISTS deployments_with_outputs;
