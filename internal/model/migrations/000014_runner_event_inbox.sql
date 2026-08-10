-- +goose Up
CREATE TABLE runner_event_inbox (
    event_id text PRIMARY KEY,
    org_id text NOT NULL,
    runner_id text NOT NULL,
    deployment_id uuid NOT NULL,
    event_type text NOT NULL,
    received_at timestamp without time zone NOT NULL DEFAULT timezone('UTC', now())
);

CREATE INDEX runner_event_inbox_received_at_idx
    ON runner_event_inbox (received_at);

-- +goose Down
DROP TABLE runner_event_inbox;
