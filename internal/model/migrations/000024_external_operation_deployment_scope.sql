-- +goose Up
ALTER TABLE external_operation_deployments
    ADD COLUMN project_uuid UUID,
    ADD COLUMN environment_uuid UUID;

UPDATE external_operation_deployments receipt
SET environment_uuid = environment.id
FROM deployments deployment
JOIN deployment_environments environment ON environment.id = deployment.de_id
WHERE deployment.id = receipt.deployment_id;

-- Project UUID was not persisted by earlier receipts and must not
-- be guessed from a mutable slug. New receipts always capture both UUIDs.

-- +goose Down
ALTER TABLE external_operation_deployments
    DROP COLUMN environment_uuid,
    DROP COLUMN project_uuid;
