-- +goose Up
ALTER TABLE deployments ADD COLUMN native_target JSONB
    CHECK (native_target IS NULL OR jsonb_typeof(native_target) = 'object');
ALTER TABLE deployment_admission_receipts ADD COLUMN native_target JSONB
    CHECK (native_target IS NULL OR jsonb_typeof(native_target) = 'object');
ALTER TABLE external_deployment_plan_snapshots ADD COLUMN native_target JSONB
    CHECK (native_target IS NULL OR jsonb_typeof(native_target) = 'object');

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployments WHERE native_target IS NOT NULL)
       OR EXISTS (SELECT 1 FROM deployment_admission_receipts WHERE native_target IS NOT NULL)
       OR EXISTS (SELECT 1 FROM external_deployment_plan_snapshots WHERE native_target IS NOT NULL)
       OR EXISTS (SELECT 1 FROM environment_deployment_queue WHERE intent->'native_target' IS NOT NULL AND intent->'native_target' <> 'null'::jsonb)
       OR EXISTS (SELECT 1 FROM deployment_pipeline_runs WHERE plan_capture->'native_target' IS NOT NULL AND plan_capture->'native_target' <> 'null'::jsonb) THEN
        RAISE EXCEPTION 'cannot discard immutable Native execution target evidence';
    END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE external_deployment_plan_snapshots DROP COLUMN native_target;
ALTER TABLE deployment_admission_receipts DROP COLUMN native_target;
ALTER TABLE deployments DROP COLUMN native_target;
