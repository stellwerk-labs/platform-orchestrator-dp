-- +goose Up
ALTER TABLE deployment_admission_receipts
    ADD COLUMN native_admission_watermark JSONB
        CHECK (native_admission_watermark IS NULL OR jsonb_typeof(native_admission_watermark) = 'object');
ALTER TABLE deployment_admission_receipts DROP CONSTRAINT deployment_admission_receipts_mode_check;
ALTER TABLE deployment_admission_receipts ADD CONSTRAINT deployment_admission_receipts_mode_check
    CHECK (mode IN ('deploy', 'rollback', 'destroy', 'plan_only', 'rollback_plan'));
CREATE INDEX deployment_admission_receipts_latest_native_knowledge
    ON deployment_admission_receipts (org_id, project_uuid, environment_uuid, core_epoch DESC)
    WHERE disposition = 'committed';
CREATE INDEX deployment_admission_receipts_unresolved_native_knowledge
    ON deployment_admission_receipts (org_id, project_uuid, environment_uuid)
    WHERE core_settled_at IS NULL OR core_epoch<=0 OR
          (disposition='absent' AND (terminal_outcome IS DISTINCT FROM 'absent' OR deployment_id IS NOT NULL)) OR
          (disposition='committed' AND (deployment_id IS NULL OR terminal_outcome IS NULL OR terminal_outcome NOT IN ('succeeded','failed','cancelled','terminated'))) OR
          disposition NOT IN ('absent','committed');

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployment_admission_receipts
               WHERE native_admission_watermark IS NOT NULL OR mode IN ('plan_only','rollback_plan')) THEN
        RAISE EXCEPTION 'cannot discard retained Native admission predecessor or Plan evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP INDEX deployment_admission_receipts_unresolved_native_knowledge;
DROP INDEX deployment_admission_receipts_latest_native_knowledge;
ALTER TABLE deployment_admission_receipts DROP CONSTRAINT deployment_admission_receipts_mode_check;
ALTER TABLE deployment_admission_receipts ADD CONSTRAINT deployment_admission_receipts_mode_check
    CHECK (mode IN ('deploy', 'rollback', 'destroy'));
ALTER TABLE deployment_admission_receipts DROP COLUMN native_admission_watermark;
