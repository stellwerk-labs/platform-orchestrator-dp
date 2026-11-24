-- +goose Up
ALTER TABLE deployment_pipelines ADD COLUMN parent_pipeline_id uuid REFERENCES deployment_pipelines(id);

-- +goose Down
ALTER TABLE deployment_pipelines DROP COLUMN parent_pipeline_id;
