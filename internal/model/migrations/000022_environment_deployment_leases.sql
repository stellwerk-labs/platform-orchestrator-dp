-- +goose Up
CREATE TABLE IF NOT EXISTS environment_deployment_leases (
    environment_id UUID PRIMARY KEY,
    org_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    env_id TEXT NOT NULL,
    holder_id UUID NOT NULL,
    purpose TEXT NOT NULL,
    deployment_id UUID,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    UNIQUE (org_id, project_id, env_id)
);

CREATE INDEX IF NOT EXISTS environment_deployment_leases_expiry
    ON environment_deployment_leases (expires_at);

-- Existing executing work may still own a live Runner process when this
-- version first starts. Fence it conservatively until the normal worker can
-- renew the lease or its maximum execution window expires.
INSERT INTO environment_deployment_leases
    (environment_id, org_id, project_id, env_id, holder_id, purpose, deployment_id, expires_at)
SELECT environment.id, environment.org_id, environment.project_id, environment.env_id,
       deployment.id, 'deployment', deployment.id, now() + INTERVAL '65 minutes'
FROM deployment_environments environment
JOIN deployments deployment ON deployment.id = environment.last_executing_deployment_id
WHERE deployment.status = 'executing'
ON CONFLICT (environment_id) DO NOTHING;

-- +goose Down
DROP TABLE IF EXISTS environment_deployment_leases;
