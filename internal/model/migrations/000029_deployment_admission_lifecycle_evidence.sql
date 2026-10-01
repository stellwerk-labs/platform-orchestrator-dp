-- +goose Up
-- Version 28 may already be applied without these lifecycle fields. Keep
-- that migration immutable and reconcile both such databases and fresh
-- ones additively here.
ALTER TABLE deployment_admission_receipts
    ADD COLUMN IF NOT EXISTS carry_forward_module_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS confirmed_restricted_version_uuids JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE deployment_admission_receipts
    ALTER COLUMN carry_forward_module_versions DROP DEFAULT,
    ALTER COLUMN confirmed_restricted_version_uuids DROP DEFAULT;

-- +goose Down
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM deployment_admission_receipts
        WHERE carry_forward_module_versions <> '{}'::jsonb
           OR confirmed_restricted_version_uuids <> '[]'::jsonb
    ) THEN
        RAISE EXCEPTION 'cannot remove Deployment admission lifecycle evidence while populated receipts exist';
    END IF;
END $$;

ALTER TABLE deployment_admission_receipts
    DROP COLUMN confirmed_restricted_version_uuids,
    DROP COLUMN carry_forward_module_versions;
