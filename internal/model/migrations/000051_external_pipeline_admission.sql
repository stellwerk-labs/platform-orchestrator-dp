-- +goose Up
ALTER TABLE deployment_admission_receipts ADD COLUMN external_dispatch JSONB NOT NULL DEFAULT 'null'::jsonb;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM deployment_admission_receipts WHERE external_dispatch<>'null'::jsonb) THEN
  RAISE EXCEPTION 'cannot discard exact external dispatch admission provenance';
 END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE deployment_admission_receipts DROP COLUMN external_dispatch;
