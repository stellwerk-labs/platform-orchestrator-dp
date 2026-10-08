-- +goose Up
-- halt_on_failure: the request must not start when the Deployment it waited
-- for failed. enqueued_after_deployment_id: the Environment's current
-- Deployment when such a request was enqueued, if that Deployment had already
-- ended; the request did not wait for it, so it never halts on it.
ALTER TABLE environment_deployment_queue
    ADD COLUMN halt_on_failure BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN enqueued_after_deployment_id UUID,
    ADD CONSTRAINT environment_deployment_queue_enqueued_after_needs_halt
        CHECK (halt_on_failure OR enqueued_after_deployment_id IS NULL);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM environment_deployment_queue WHERE state = 'queued' AND halt_on_failure) THEN
        RAISE EXCEPTION 'cannot discard halt_on_failure of queued Deployment requests';
    END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE environment_deployment_queue
    DROP CONSTRAINT environment_deployment_queue_enqueued_after_needs_halt,
    DROP COLUMN enqueued_after_deployment_id,
    DROP COLUMN halt_on_failure;
