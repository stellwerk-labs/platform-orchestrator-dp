-- +goose Up
ALTER TABLE external_operation_deployments
    ADD COLUMN plan_snapshot_id UUID,
    ADD COLUMN base_deployment_id UUID;

-- +goose Down
ALTER TABLE external_operation_deployments
    DROP COLUMN base_deployment_id,
    DROP COLUMN plan_snapshot_id;
