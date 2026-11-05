-- +goose Up
-- pg_restore deliberately uses an empty search_path. The Draft CHECK functions
-- must resolve their nested reference validator in the table's own schema.
-- Keep pg_catalog first and never inherit a caller-controlled schema path.
-- +goose StatementBegin
DO $$
DECLARE application_schema TEXT;
BEGIN
    SELECT n.nspname INTO STRICT application_schema
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = 'deployments'::regclass;

    EXECUTE format(
        'ALTER FUNCTION %I.valid_draft_test_artifacts(JSONB,UUID) SET search_path TO pg_catalog, %I, pg_temp',
        application_schema, application_schema);
    EXECUTE format(
        'ALTER FUNCTION %I.valid_draft_test_graph(JSONB,UUID) SET search_path TO pg_catalog, %I, pg_temp',
        application_schema, application_schema);
END;
$$;
-- +goose StatementEnd

-- +goose Down
-- These paths are compatible with older binaries. Retain this restore-safety
-- fix rather than reintroducing caller-dependent validation on a logical Down.
SELECT 1;
