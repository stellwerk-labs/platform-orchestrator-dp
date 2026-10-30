-- +goose Up
-- The schema fence also prevents older binaries from silently discarding
-- interface metadata when reading and re-serializing retained JSON snapshots.
-- +goose StatementBegin
CREATE FUNCTION valid_resource_type_contract_snapshots(document JSONB) RETURNS BOOLEAN
IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE artifact JSONB;
BEGIN
    -- Older Deployments could store an absent artifact list as JSON null.
    IF document IS NULL OR document = 'null'::jsonb THEN
        RETURN TRUE;
    END IF;
    IF jsonb_typeof(document) <> 'array' THEN
        RETURN FALSE;
    END IF;
    FOR artifact IN SELECT value FROM jsonb_array_elements(document) LOOP
        IF jsonb_typeof(artifact) <> 'object' THEN
            RETURN FALSE;
        END IF;
        IF artifact ? 'output_schema' AND jsonb_typeof(artifact->'output_schema') NOT IN ('object', 'null') THEN
            RETURN FALSE;
        END IF;
        IF artifact ? 'resource_type_version' OR artifact ? 'resource_type_version_uuid' THEN
            IF jsonb_typeof(artifact->'resource_type_version') IS DISTINCT FROM 'number'
               OR jsonb_typeof(artifact->'resource_type_version_uuid') IS DISTINCT FROM 'string'
               OR artifact->>'resource_type_version' !~ '^[1-9][0-9]*$'
               OR artifact->>'resource_type_version_uuid' !~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
               OR artifact->>'resource_type_version_uuid' = '00000000-0000-0000-0000-000000000000' THEN
                RETURN FALSE;
            END IF;
        END IF;
    END LOOP;
    RETURN TRUE;
END;
$$;
-- +goose StatementEnd

ALTER TABLE deployments ADD CONSTRAINT deployment_resource_type_contract_snapshots_valid
    CHECK (valid_resource_type_contract_snapshots(module_artifacts));
ALTER TABLE external_deployment_plan_snapshots ADD CONSTRAINT plan_resource_type_contract_snapshots_valid
    CHECK (valid_resource_type_contract_snapshots(module_artifacts));

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (
        SELECT 1 FROM (
            SELECT module_artifacts AS artifacts FROM deployments
            UNION ALL
            SELECT module_artifacts FROM external_deployment_plan_snapshots
        ) retained
        CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(retained.artifacts) = 'array'
            THEN retained.artifacts ELSE '[]'::jsonb END) artifact
        WHERE artifact ?| ARRAY['resource_type_version', 'resource_type_version_uuid', 'output_schema']
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while Resource Type contract execution snapshots exist';
    END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE external_deployment_plan_snapshots DROP CONSTRAINT plan_resource_type_contract_snapshots_valid;
ALTER TABLE deployments DROP CONSTRAINT deployment_resource_type_contract_snapshots_valid;
DROP FUNCTION valid_resource_type_contract_snapshots(JSONB);
