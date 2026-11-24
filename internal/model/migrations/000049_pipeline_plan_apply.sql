-- +goose Up
ALTER TABLE deployment_pipeline_runs
    ADD COLUMN plan_capture JSONB,
    ADD COLUMN source_plan_id UUID UNIQUE REFERENCES deployments(id),
    ADD COLUMN source_plan_subject TEXT,
    ADD COLUMN replanned_from_id UUID REFERENCES deployments(id),
    ADD CONSTRAINT pipeline_plan_source_complete CHECK
        ((source_plan_id IS NULL) = (source_plan_subject IS NULL));

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployment_pipeline_runs WHERE plan_capture IS NOT NULL OR source_plan_id IS NOT NULL OR replanned_from_id IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot discard saved-plan authority and single-use bindings';
    END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE deployment_pipeline_runs
    DROP CONSTRAINT pipeline_plan_source_complete,
    DROP COLUMN replanned_from_id,
    DROP COLUMN source_plan_subject,
    DROP COLUMN source_plan_id,
    DROP COLUMN plan_capture;
