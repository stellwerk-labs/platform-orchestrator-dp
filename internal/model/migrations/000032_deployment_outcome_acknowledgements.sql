-- +goose Up
-- One audited operator acknowledgement per failed plugin-dispatched Deployment
-- whose Runner outcome is unknown. It replaces missing stop proof for that
-- Deployment only. There is deliberately no foreign key: the audit record
-- outlives normal Deployment retention.
CREATE TABLE deployment_outcome_acknowledgements (
    deployment_id UUID PRIMARY KEY,
    org_id TEXT NOT NULL,
    environment_id UUID NOT NULL,
    plugin_id TEXT NOT NULL,
    operation_ref TEXT NOT NULL,
    actor_id UUID NOT NULL,
    reason TEXT NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 1000),
    acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployment_outcome_acknowledgements) THEN
        RAISE EXCEPTION 'cannot discard audited Deployment outcome acknowledgements';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE deployment_outcome_acknowledgements;
