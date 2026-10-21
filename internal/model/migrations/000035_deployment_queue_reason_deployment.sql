-- +goose Up
-- reason_deployment_id: the Deployment a cancelled, expired or rejected item's
-- reason refers to, for example the failed Deployment a halt_on_failure item
-- waited for, so clients need not parse reason_message. Like deployment_id it
-- is a plain reference without a foreign key.
ALTER TABLE environment_deployment_queue
    ADD COLUMN reason_deployment_id UUID,
    ADD CONSTRAINT environment_deployment_queue_reason_deployment_needs_reason
        CHECK (reason_deployment_id IS NULL OR reason_code IS NOT NULL);

-- +goose Down
-- Only settled items carry the column, so dropping it changes no waiting item.
ALTER TABLE environment_deployment_queue
    DROP CONSTRAINT environment_deployment_queue_reason_deployment_needs_reason,
    DROP COLUMN reason_deployment_id;
