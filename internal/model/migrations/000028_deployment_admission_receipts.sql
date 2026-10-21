-- +goose Up
CREATE TABLE deployment_admission_receipts (
    org_id TEXT NOT NULL,
    admission_id UUID NOT NULL,
    project_uuid UUID NOT NULL,
    environment_uuid UUID NOT NULL,
    actor_id UUID NOT NULL,
    request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
    mode TEXT NOT NULL CHECK (mode IN ('deploy', 'rollback', 'destroy')),
    module_versions JSONB NOT NULL,
    core_epoch BIGINT NOT NULL DEFAULT 0 CHECK (core_epoch >= 0),
    disposition TEXT NOT NULL CHECK (disposition IN ('pending', 'committed', 'absent')),
    deployment_id UUID UNIQUE,
    terminal_outcome TEXT CHECK (terminal_outcome IN ('succeeded', 'failed', 'cancelled', 'terminated', 'absent')),
    core_settled_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, admission_id),
    CHECK ((disposition = 'pending' AND deployment_id IS NULL AND terminal_outcome IS NULL AND core_epoch = 0 AND core_settled_at IS NULL) OR
           (disposition = 'committed' AND deployment_id IS NOT NULL AND core_epoch > 0 AND terminal_outcome IS DISTINCT FROM 'absent') OR
           (disposition = 'absent' AND deployment_id IS NULL AND terminal_outcome = 'absent')),
    CHECK (core_settled_at IS NULL OR core_epoch > 0)
);

-- Receipts deliberately outlive normal Deployment retention. They are the
-- durable outcome authority for Core admission guards, not a Deployment FK.
CREATE INDEX deployment_admission_receipts_deployment
    ON deployment_admission_receipts (deployment_id) WHERE deployment_id IS NOT NULL;
CREATE INDEX deployment_admission_receipts_recovery
    ON deployment_admission_receipts (created_at) WHERE core_settled_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS deployment_admission_receipts;
