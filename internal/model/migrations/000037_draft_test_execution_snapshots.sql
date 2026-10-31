-- +goose Up
-- The version fence prevents older binaries from dropping executable provenance.
-- +goose StatementBegin
CREATE FUNCTION valid_draft_test_reference(reference JSONB) RETURNS BOOLEAN
IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE key TEXT;
BEGIN
    IF jsonb_typeof(reference) IS DISTINCT FROM 'object'
       OR jsonb_typeof(reference->'draft_revision') IS DISTINCT FROM 'number'
       OR reference->>'draft_revision' !~ '^[1-9][0-9]*$' THEN RETURN FALSE; END IF;
    FOREACH key IN ARRAY ARRAY['snapshot_uuid','module_uuid','draft_uuid','project_uuid','environment_uuid'] LOOP
        IF jsonb_typeof(reference->key) IS DISTINCT FROM 'string'
           OR reference->>key !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR reference->>key = '00000000-0000-0000-0000-000000000000' THEN RETURN FALSE; END IF;
    END LOOP;
    RETURN TRUE;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION valid_draft_test_artifacts(document JSONB, expected_environment UUID DEFAULT NULL) RETURNS BOOLEAN
IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE artifact JSONB;
BEGIN
    IF document IS NULL OR document='null'::jsonb THEN RETURN TRUE; END IF;
    IF jsonb_typeof(document)<>'array' THEN RETURN FALSE; END IF;
    FOR artifact IN SELECT value FROM jsonb_array_elements(document) LOOP
        IF artifact ? 'draft_test' THEN
            IF NOT valid_draft_test_reference(artifact->'draft_test')
               OR (expected_environment IS NOT NULL AND artifact->'draft_test'->>'environment_uuid' IS DISTINCT FROM expected_environment::text)
               OR artifact->>'module_uuid' IS DISTINCT FROM artifact->'draft_test'->>'module_uuid'
               OR artifact->>'version_uuid' IS DISTINCT FROM artifact->'draft_test'->>'snapshot_uuid'
               OR artifact->>'version' IS DISTINCT FROM artifact->'draft_test'->>'snapshot_uuid'
               OR NOT (artifact ?& ARRAY['resource_type_version','resource_type_version_uuid'])
               OR artifact ?| ARRAY['semantic_version','migration_generation','confirmed_restricted_version_uuid'] THEN RETURN FALSE; END IF;
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION retained_graph_document(raw BYTEA) RETURNS JSONB
IMMUTABLE LANGUAGE plpgsql AS $$
BEGIN
    RETURN convert_from(raw,'UTF8')::jsonb;
EXCEPTION WHEN invalid_text_representation OR character_not_in_repertoire THEN
    -- Historical fixtures and opaque legacy records need not be JSON graphs.
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION valid_draft_test_graph(document JSONB, expected_environment UUID DEFAULT NULL) RETURNS BOOLEAN
IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE node JSONB;
BEGIN
    IF document IS NULL OR jsonb_typeof(document->'nodes') IS DISTINCT FROM 'object' THEN RETURN TRUE; END IF;
    FOR node IN SELECT value FROM jsonb_each(document->'nodes') LOOP
        IF node->'config' ? 'draft_test' THEN
            IF NOT valid_draft_test_reference(node->'config'->'draft_test')
               OR (expected_environment IS NOT NULL AND node->'config'->'draft_test'->>'environment_uuid' IS DISTINCT FROM expected_environment::text)
               OR node->'config'->>'version_id' IS DISTINCT FROM node->'config'->'draft_test'->>'snapshot_uuid'
               OR NOT (node->'config' ?& ARRAY['resource_type_version','resource_type_version_uuid']) THEN RETURN FALSE; END IF;
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION valid_draft_test_selection(selection JSONB, targets JSONB) RETURNS BOOLEAN
IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE entry RECORD;
BEGIN
    IF selection='null'::jsonb THEN RETURN TRUE; END IF;
    IF jsonb_typeof(selection) IS DISTINCT FROM 'object' THEN RETURN FALSE; END IF;
    FOR entry IN SELECT key,value FROM jsonb_each(selection) LOOP
        IF entry.key !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR entry.key='00000000-0000-0000-0000-000000000000'
           OR jsonb_typeof(entry.value) IS DISTINCT FROM 'string'
           OR entry.value#>>'{}' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
           OR entry.value#>>'{}'='00000000-0000-0000-0000-000000000000'
           OR targets->entry.key IS DISTINCT FROM entry.value THEN RETURN FALSE; END IF;
    END LOOP;
    RETURN TRUE;
END;
$$;
-- +goose StatementEnd

ALTER TABLE deployments ADD CONSTRAINT deployment_draft_test_artifacts_valid CHECK (valid_draft_test_artifacts(module_artifacts,de_id));
ALTER TABLE deployments ADD CONSTRAINT deployment_draft_test_graph_valid CHECK (valid_draft_test_graph(retained_graph_document(graph),de_id));
ALTER TABLE deployment_admission_receipts ADD COLUMN draft_test_snapshots JSONB NOT NULL DEFAULT 'null'::jsonb;
ALTER TABLE deployment_admission_receipts ADD CONSTRAINT admission_draft_test_selection_valid CHECK (valid_draft_test_selection(draft_test_snapshots,module_versions));
-- Add-on plans cannot acquire Draft execution authority.
ALTER TABLE external_deployment_plan_snapshots ADD CONSTRAINT external_plan_no_draft_tests CHECK (
    NOT jsonb_path_exists(module_artifacts,'$[*].draft_test') AND NOT jsonb_path_exists(graph,'$.nodes.*.config.draft_test'));

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM deployments WHERE jsonb_path_exists(module_artifacts,'$[*].draft_test')
        OR jsonb_path_exists(retained_graph_document(graph),'$.nodes.*.config.draft_test'))
        OR EXISTS (SELECT 1 FROM deployment_admission_receipts WHERE draft_test_snapshots NOT IN ('null'::jsonb,'{}'::jsonb))
        OR EXISTS (SELECT 1 FROM environment_deployment_queue WHERE intent ? 'draft_test_snapshots' AND intent->'draft_test_snapshots' NOT IN ('null'::jsonb,'{}'::jsonb)) THEN
        RAISE EXCEPTION 'cannot downgrade while Draft test execution history exists';
    END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE external_deployment_plan_snapshots DROP CONSTRAINT external_plan_no_draft_tests;
ALTER TABLE deployment_admission_receipts DROP CONSTRAINT admission_draft_test_selection_valid;
ALTER TABLE deployment_admission_receipts DROP COLUMN draft_test_snapshots;
ALTER TABLE deployments DROP CONSTRAINT deployment_draft_test_graph_valid;
ALTER TABLE deployments DROP CONSTRAINT deployment_draft_test_artifacts_valid;
DROP FUNCTION valid_draft_test_selection(JSONB,JSONB);
DROP FUNCTION valid_draft_test_graph(JSONB,UUID);
DROP FUNCTION retained_graph_document(BYTEA);
DROP FUNCTION valid_draft_test_artifacts(JSONB,UUID);
DROP FUNCTION valid_draft_test_reference(JSONB);
