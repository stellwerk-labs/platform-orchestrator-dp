-- +goose Up
CREATE TABLE deployment_pipeline_runs (
    deployment_id UUID PRIMARY KEY REFERENCES deployments(id) ON DELETE CASCADE,
    backend TEXT NOT NULL,
    definition JSONB NOT NULL,
    progress JSONB NOT NULL,
    site_revision BIGINT NOT NULL DEFAULT 0,
    settled BOOLEAN NOT NULL DEFAULT false,
    command BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployment_pipeline_runs) THEN
        RAISE EXCEPTION 'cannot discard canonical pipeline execution and command evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE deployment_pipeline_runs;
