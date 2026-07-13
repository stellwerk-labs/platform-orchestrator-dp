-- +goose Up

CREATE TABLE IF NOT EXISTS metadata_keys (
    org_id text NOT NULL,
    name text NOT NULL,
    type text NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    description text,
    format text,
    pattern text,
    CONSTRAINT metadata_keys_pkey PRIMARY KEY (org_id, name)
);

-- +goose Down

DROP TABLE IF EXISTS metadata_keys;
