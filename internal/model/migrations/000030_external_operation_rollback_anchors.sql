-- +goose Up
ALTER TABLE external_operation_deployments
    ADD COLUMN forward_deployment_id UUID,
    ADD COLUMN retry_of_rollback_deployment_id UUID;
CREATE TABLE external_operation_rollback_anchors (
    org_id TEXT NOT NULL,
    plugin_id TEXT NOT NULL,
    operation_ref TEXT NOT NULL,
    environment_uuid UUID NOT NULL,
    project_uuid UUID NOT NULL,
    project_id TEXT NOT NULL,
    env_id TEXT NOT NULL,
    first_forward_deployment_id UUID NOT NULL UNIQUE,
    rollback_base_deployment_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, plugin_id, operation_ref, environment_uuid)
);

-- Receipts and anchors retain identity after an Environment is explicitly
-- deleted. Ordinary cleanup must not turn a historical operation into a new
-- first admission; missing retained snapshots instead fail closed.
INSERT INTO external_operation_rollback_anchors
    (org_id, plugin_id, operation_ref, environment_uuid, project_uuid, project_id, env_id,
     first_forward_deployment_id, rollback_base_deployment_id)
SELECT r.org_id, r.plugin_id, r.operation_ref, r.environment_uuid, r.project_uuid, r.project_id, r.env_id,
       r.deployment_id, r.base_deployment_id
FROM external_operation_deployments r
JOIN deployments f ON f.id=r.deployment_id AND f.mode='deploy'
JOIN deployments b ON b.id=r.base_deployment_id AND b.status='succeeded' AND b.completed_at IS NOT NULL
JOIN deployment_environments e ON e.id=f.de_id AND e.id=b.de_id
WHERE r.service_action='external-deployment.admit'
  AND r.project_uuid IS NOT NULL AND r.environment_uuid=e.id
  AND r.org_id=e.org_id AND r.project_id=e.project_id AND r.env_id=e.env_id
  AND b.manifest IS NOT NULL AND octet_length(b.manifest)>0 AND b.manifest<>convert_to('null','UTF8')
  AND b.tofu IS NOT NULL AND octet_length(b.tofu)>0
  AND b.graph IS NOT NULL AND octet_length(b.graph)>0 AND b.graph<>convert_to('null','UTF8')
  AND (SELECT count(*) FROM external_operation_deployments candidates
       WHERE candidates.org_id=r.org_id AND candidates.plugin_id=r.plugin_id
         AND candidates.operation_ref=r.operation_ref
         AND (candidates.environment_uuid=r.environment_uuid OR (candidates.project_id=r.project_id AND candidates.env_id=r.env_id))
         AND candidates.service_action='external-deployment.admit')=1;

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM external_operation_rollback_anchors a
               JOIN external_operation_deployments r USING (org_id, plugin_id, operation_ref, environment_uuid)
               WHERE r.service_action='external-deployment.admit' AND r.deployment_id<>a.first_forward_deployment_id) THEN
        RAISE EXCEPTION 'cannot remove immutable rollback anchors after forward retry';
    END IF;
    IF EXISTS (SELECT 1 FROM external_operation_deployments WHERE retry_of_rollback_deployment_id IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot remove explicit rollback retry predecessor evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE external_operation_rollback_anchors;
ALTER TABLE external_operation_deployments
    DROP COLUMN retry_of_rollback_deployment_id,
    DROP COLUMN forward_deployment_id;
