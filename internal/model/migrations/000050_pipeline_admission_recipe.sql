-- +goose Up
ALTER TABLE deployment_admission_receipts ADD COLUMN pipeline JSONB NOT NULL DEFAULT 'null'::jsonb;
ALTER TABLE deployment_admission_receipts ADD CONSTRAINT admission_pipeline_object CHECK (pipeline='null'::jsonb OR jsonb_typeof(pipeline)='object');
-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM deployment_admission_receipts WHERE pipeline<>'null'::jsonb) THEN
  RAISE EXCEPTION 'cannot discard pipeline admission authority';
 END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE deployment_admission_receipts DROP COLUMN pipeline;
