package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

type DeploymentMode string

const (
	DeploymentModeDeployPlan   DeploymentMode = "plan_only"
	DeploymentModeDeploy       DeploymentMode = "deploy"
	DeploymentModeRollback     DeploymentMode = "rollback"
	DeploymentModeRollbackPlan DeploymentMode = "rollback_plan"
	DeploymentModeDestroy      DeploymentMode = "destroy"
)

type DeploymentStatus string

const (
	DeploymentStatusExecuting DeploymentStatus = "executing"
	DeploymentStatusFailed    DeploymentStatus = "failed"
	DeploymentStatusSucceeded DeploymentStatus = "succeeded"
)

const idempotencyDigestTimeLimit = time.Hour * 24

// DefaultEnvironmentLeaseTTL outlives the Runner's one-hour status-check
// window. It fences a replacement mutation while an older Runner may still be
// changing the same Environment.
const DefaultEnvironmentLeaseTTL = 65 * time.Minute

type DeploymentSummary struct {
	OrgId             string
	ProjectId         string
	EnvId             string
	DeploymentEnvUuid uuid.UUID
	Id                uuid.UUID
	Revision          int

	Mode          DeploymentMode
	RollbackToId  opt.Opt[uuid.UUID]
	CreatedAt     time.Time
	CreatedBy     uuid.UUID
	CompletedAt   opt.Opt[time.Time]
	Status        DeploymentStatus
	StatusMessage string

	RunnerId                   string
	Metrics                    DeploymentMetrics
	EncryptedOutputsRecipient  opt.Opt[string]
	EncryptedLogsRecipient     opt.Opt[string]
	RunnerLogLevel             string
	ModuleArtifacts            []ModuleArtifactRequirement
	ModuleVersionRequestDigest string
	// Internal only: caps buffered Runner commands at the immutable first attempt budget.
	DispatchDeadline time.Time
}

type UpdateDeploymentStatusAndOutputsParams struct {
	Status        DeploymentStatus
	StatusMessage string

	Outputs opt.Opt[string]
	Metrics DeploymentMetrics

	// ExpectedRevision will reject the update if the deployment is not at the same revision number
	ExpectedRevision opt.Opt[int64]
	// Set only by an authenticated Runner result, never an operator/watchdog failure.
	RunnerCompletionProven bool
}

type EncodedDeploymentManifest json.RawMessage
type EncodedDeploymentGraph json.RawMessage
type RawTofu []byte

type CreateDeploymentParams struct {
	CreatedBy                  uuid.UUID
	DeploymentEnvUuid          uuid.UUID
	Mode                       DeploymentMode
	Manifest                   EncodedDeploymentManifest
	RollbackToId               opt.Opt[uuid.UUID]
	Graph                      EncodedDeploymentGraph
	Tofu                       RawTofu
	IdempotencyKeyDigest       opt.Opt[string]
	RunnerId                   string
	EncryptedOutputsRecipient  opt.Opt[string]
	EncryptedLogsRecipient     opt.Opt[string]
	Metrics                    DeploymentMetrics
	RunnerLogLevel             string
	ModuleArtifacts            []ModuleArtifactRequirement
	ModuleVersionRequestDigest string
	// Optional captured-base recovery CAS, checked after the normal lease is held.
	ExpectedCurrentDeploymentID opt.Opt[uuid.UUID]
	RetryOfRollbackDeploymentID opt.Opt[uuid.UUID]
}

type ModuleArtifactRequirement struct {
	ModuleID                  string    `json:"module_id"`
	ModuleUUID                uuid.UUID `json:"module_uuid,omitempty"`
	Version                   string    `json:"version"`
	VersionUUID               uuid.UUID `json:"version_uuid,omitempty"`
	SemanticVersion           string    `json:"semantic_version,omitempty"`
	MigrationGeneration       string    `json:"migration_generation,omitempty"`
	Source                    string    `json:"source"`
	ArtifactDigest            string    `json:"artifact_digest,omitempty"`
	RetainedArtifactException bool      `json:"retained_artifact_exception,omitempty"`
	// Captured only after exact-version confirmation and scoped authorization.
	// Background bundle reconstruction must not drop this operation's authority.
	ConfirmedRestrictedVersionUUID *uuid.UUID `json:"confirmed_restricted_version_uuid,omitempty"`
}

type EnvironmentRef struct {
	ProjectID string
	EnvID     string
}

type EnvironmentDeploymentReadiness struct {
	EnvironmentRef
	EnvironmentUUID           opt.Opt[uuid.UUID]
	LatestDeploymentID        opt.Opt[uuid.UUID]
	LatestStatus              opt.Opt[DeploymentStatus]
	LatestMode                opt.Opt[DeploymentMode]
	ActiveLeaseUntil          opt.Opt[time.Time]
	EffectiveBaseDeploymentID opt.Opt[uuid.UUID]
	ActiveModules             []ModuleArtifactRequirement
}

type ListDeploymentsParams struct {
	ProjectId opt.Opt[string]
	EnvId     opt.Opt[string]

	CreatedBefore opt.Opt[time.Time]
	ByMode        []DeploymentMode
	ByStatus      []DeploymentStatus
}

type ListLastDeploymentsParams struct {
	ProjectId opt.Opt[string]
	EnvId     opt.Opt[string]

	StateChangeOnly bool
}

type GetLastDeploymentParams struct {
	StateChangeOnly bool
}

type DeploymentMetrics struct {
	Workloads          int `json:"workloads"`
	ResourceNodes      int `json:"resource_nodes"`
	TfResources        int `json:"tf_total"`
	TfResourcesAdded   int `json:"tf_added"`
	TfResourcesRemoved int `json:"tf_removed"`
	TfResourcesChanged int `json:"tf_changed"`
}

func ConvertDeploymentToEventData(d *DeploymentSummary) genevents.DeploymentChangedData {
	return genevents.DeploymentChangedData{
		DeploymentId: d.Id,
		EnvId:        d.EnvId,
		EnvUuid:      d.DeploymentEnvUuid,
		OrgId:        d.OrgId,
		ProjectId:    d.ProjectId,
		Revision:     d.Revision,
		Status:       ref.Ref(string(d.Status)),
	}
}

func ConvertDeploymentToEventPayload(d *DeploymentSummary) json.RawMessage {
	raw, _ := json.Marshal(events.CloudEvent[genevents.DeploymentChangedData]{
		Type: genevents.IoPlatformOrchestratorDeploymentCreated,
		Time: d.CreatedAt,
		Data: ConvertDeploymentToEventData(d),
	})
	return raw
}

func (d *databaser) CreateDeployment(ctx context.Context, tx Tx, orgId, projectId, envId string, params CreateDeploymentParams) (*DeploymentSummary, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	if params.ExpectedCurrentDeploymentID.IsSet() && (params.ExpectedCurrentDeploymentID.Must() == uuid.Nil || params.Mode != DeploymentModeRollback || !params.RollbackToId.IsSet()) {
		return nil, NewErrBadRequest("expected current Deployment requires captured-base rollback")
	}
	if params.RetryOfRollbackDeploymentID.IsSet() && (!params.ExpectedCurrentDeploymentID.IsSet() || params.RetryOfRollbackDeploymentID.Must() == uuid.Nil || params.RetryOfRollbackDeploymentID.Must() != params.ExpectedCurrentDeploymentID.Must()) {
		return nil, NewErrBadRequest("explicit rollback retry must identify the expected current head")
	}

	out := &DeploymentSummary{
		OrgId:                      orgId,
		ProjectId:                  projectId,
		EnvId:                      envId,
		DeploymentEnvUuid:          params.DeploymentEnvUuid,
		Id:                         uuid.New(),
		Mode:                       params.Mode,
		RollbackToId:               params.RollbackToId,
		CreatedAt:                  time.Now().UTC(),
		CreatedBy:                  params.CreatedBy,
		Status:                     DeploymentStatusExecuting,
		StatusMessage:              "Deploying...",
		RunnerId:                   params.RunnerId,
		EncryptedOutputsRecipient:  params.EncryptedOutputsRecipient,
		EncryptedLogsRecipient:     params.EncryptedLogsRecipient,
		Metrics:                    params.Metrics,
		Revision:                   1,
		ModuleArtifacts:            params.ModuleArtifacts,
		ModuleVersionRequestDigest: params.ModuleVersionRequestDigest,
	}

	if RequiresEnvironmentLease(params.Mode) {
		if err := LockEnvironmentMutation(ctx, tx, out.DeploymentEnvUuid); err != nil {
			return nil, err
		}
		var holder uuid.UUID
		if err := tx.QueryRowContext(ctx, `INSERT INTO environment_deployment_leases
			(environment_id, org_id, project_id, env_id, holder_id, purpose, deployment_id, expires_at)
			VALUES ($1, $2, $3, $4, $5, 'deployment', $5, now() + $6::interval)
			ON CONFLICT (environment_id) DO UPDATE SET
				org_id=EXCLUDED.org_id, project_id=EXCLUDED.project_id, env_id=EXCLUDED.env_id,
				holder_id=EXCLUDED.holder_id, purpose=EXCLUDED.purpose,
				deployment_id=EXCLUDED.deployment_id, acquired_at=now(), heartbeat_at=now(), expires_at=EXCLUDED.expires_at
			WHERE environment_deployment_leases.expires_at <= now()
			RETURNING holder_id`, out.DeploymentEnvUuid, orgId, projectId, envId, out.Id, DefaultEnvironmentLeaseTTL.String()).Scan(&holder); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, NewErrConflict("environment has an active deployment lease")
			}
			return nil, errors.Wrap(err, "failed to acquire environment deployment lease")
		}
	}

	{
		var lastDeploymentId uuid.UUID
		var lastDeploymentStatus string
		var lastStateDeploymentID uuid.UUID
		if err := tx.QueryRowContext(
			ctx,
			`SELECT d.id, d.status, e.last_state_deployment_id FROM deployment_environments e INNER JOIN deployments d ON d.de_id = e.id AND d.id = e.last_executing_deployment_id WHERE e.org_id = $1 AND e.project_id = $2 AND e.env_id = $3 FOR UPDATE`,
			orgId, projectId, envId,
		).Scan(&lastDeploymentId, &lastDeploymentStatus, &lastStateDeploymentID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, errors.Wrap(err, "failed to query for existing executing deployment")
			}
			if params.ExpectedCurrentDeploymentID.IsSet() {
				return nil, NewErrConflict("captured-base recovery has no current Deployment head")
			}
		} else if DeploymentStatus(lastDeploymentStatus) == DeploymentStatusExecuting {
			return nil, NewErrConflict(fmt.Sprintf("deployment '%s' is still executing", lastDeploymentId))
		}
		if lastStateDeploymentID != uuid.Nil {
			evidence, err := ReadDeploymentStopEvidence(ctx, tx, lastStateDeploymentID)
			if err != nil {
				return nil, err
			}
			if evidence.BlocksReplacement() {
				return nil, NewErrConflict(fmt.Sprintf("failed plugin-dispatched Deployment '%s' may still have an active Runner; outcome is unreconciled", lastStateDeploymentID))
			}
		}
		if params.ExpectedCurrentDeploymentID.IsSet() {
			if lastStateDeploymentID != params.ExpectedCurrentDeploymentID.Must() {
				return nil, NewErrConflict("captured-base recovery forward Deployment is no longer the current head")
			}
			forward, _, _, _, err := d.GetDeployment(ctx, tx, orgId, lastStateDeploymentID, GetModeForUpdate)
			modeMatches := err == nil && forward.Mode == DeploymentModeDeploy
			if err == nil && params.RetryOfRollbackDeploymentID.IsSet() {
				modeMatches = forward.Mode == DeploymentModeRollback && forward.Status == DeploymentStatusFailed &&
					forward.RollbackToId.IsSet() && forward.RollbackToId.Must() == params.RollbackToId.Must()
			}
			if err != nil || forward.DeploymentEnvUuid != params.DeploymentEnvUuid || !modeMatches || !forward.CompletedAt.IsSet() ||
				(forward.Status != DeploymentStatusSucceeded && forward.Status != DeploymentStatusFailed) {
				return nil, NewErrConflict("captured-base recovery requires a stopped terminal forward or explicitly retried failed rollback Deployment")
			}
			if forward.Status == DeploymentStatusFailed {
				// Watchdog/status-check failures also set CompletedAt. Only the
				// transactionally applied Runner result proves a terminal effect,
				// unless an operator acknowledged this exact unknown outcome.
				var completed bool
				if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runner_event_inbox
					WHERE org_id=$1 AND runner_id=$2 AND deployment_id=$3 AND event_type='deployment-result' FOR SHARE)
					OR EXISTS (SELECT 1 FROM deployment_late_runner_results
					WHERE org_id=$1 AND runner_id=$2 AND deployment_id=$3 FOR SHARE)
					OR EXISTS (SELECT 1 FROM deployment_dispatch_budgets WHERE deployment_id=$3 AND completion_received_at IS NOT NULL FOR SHARE)
					OR EXISTS (SELECT 1 FROM deployment_outcome_acknowledgements WHERE org_id=$1 AND deployment_id=$3 FOR SHARE)`,
					orgId, forward.RunnerId, forward.Id).Scan(&completed); err != nil {
					return nil, errors.Wrap(err, "failed to verify terminal Runner result")
				}
				if !completed {
					return nil, NewErrConflict("failed forward Deployment has no authoritative terminal Runner result")
				}
			}
			base, manifest, tofu, graph, err := d.GetDeployment(ctx, tx, orgId, params.RollbackToId.Must(), GetModeForUpdate)
			if err != nil || base.Status != DeploymentStatusSucceeded || !base.CompletedAt.IsSet() || base.DeploymentEnvUuid != params.DeploymentEnvUuid ||
				base.ProjectId != projectId || base.EnvId != envId || !CompleteDeploymentSnapshot(manifest, tofu, graph) {
				return nil, NewErrConflict("captured rollback base is not a complete retained successful Deployment")
			}
		}
	}

	statement := `INSERT INTO deployment_environments (id, org_id, project_id, env_id, last_executing_deployment_id, last_executing_deployment_at, last_state_deployment_id, last_state_deployment_at) VALUES ($1, $2, $3, $4, $5, $6, $5, $6)
		ON CONFLICT (org_id, project_id, env_id) DO UPDATE SET last_executing_deployment_id = $5, last_executing_deployment_at = $6, last_state_deployment_id = $5, last_state_deployment_at = $6`
	if params.Mode == DeploymentModeDeployPlan || params.Mode == DeploymentModeRollbackPlan {
		statement = `INSERT INTO deployment_environments (id, org_id, project_id, env_id, last_executing_deployment_id, last_executing_deployment_at, last_state_deployment_id, last_state_deployment_at) VALUES ($1, $2, $3, $4, $5, $6, $5, $6)
        	ON CONFLICT (org_id, project_id, env_id) DO UPDATE SET last_executing_deployment_id = $5, last_executing_deployment_at = $6`
	}
	if _, err := tx.ExecContext(ctx, statement, out.DeploymentEnvUuid, orgId, projectId, envId, out.Id, out.CreatedAt); err != nil {
		return nil, errors.Wrap(err, "failed to update deployment environment")
	}

	if err := tx.QueryRowContext(
		ctx,
		`INSERT INTO deployments (de_id, id, created_at, created_by, mode, status, status_message, manifest, graph, tofu, runner_id, idempotency_key_digest, encrypted_outputs_recipient, encrypted_logs_recipient, metrics, runner_log_level, rollback_to_id, module_artifacts, module_version_request_digest)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) RETURNING created_at`,
		out.DeploymentEnvUuid,
		out.Id,
		out.CreatedAt,
		out.CreatedBy,
		params.Mode,
		DeploymentStatusExecuting,
		"Deploying...",
		params.Manifest,
		params.Graph,
		params.Tofu,
		params.RunnerId,
		params.IdempotencyKeyDigest.Ref(),
		params.EncryptedOutputsRecipient.Ref(),
		params.EncryptedLogsRecipient.Ref(),
		asJson(&params.Metrics),
		params.RunnerLogLevel,
		params.RollbackToId.Ref(),
		asJson(&params.ModuleArtifacts),
		params.ModuleVersionRequestDigest,
	).Scan(&out.CreatedAt); err != nil {
		return nil, errors.Wrap(err, "failed to insert deployment")
	}

	if _, err := tx.ExecContext(ctx, `INSERT INTO deployment_dispatch_budgets (deployment_id,deployment_revision) VALUES ($1,$2)`, out.Id, out.Revision); err != nil {
		return nil, errors.Wrap(err, "failed to create Runner dispatch budget")
	}
	return out, nil
}

func (d *databaser) ListDeployments(ctx context.Context, optionalTx Tx, orgId string, pageToken string, perPage int, params ListDeploymentsParams) ([]DeploymentSummary, string, error) {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)

	beforeCreatedAt := time.Now().UTC().Add(time.Hour)
	if pageToken != "" {
		i, err := strconv.ParseInt(pageToken, 10, 64)
		if err != nil {
			return nil, "", NewErrBadRequest(errors.Wrapf(err, "failed to parse page token %q", pageToken).Error())
		}
		beforeCreatedAt = time.Unix(0, i)
	} else if params.CreatedBefore.IsSet() {
		beforeCreatedAt = params.CreatedBefore.Must()
	}

	limitPlusOne := max(1, perPage) + 1

	// nil means no filtering, empty slice means filter for nothing (return no matches)
	var byMode, byStatus pq.StringArray
	if params.ByMode != nil {
		byMode = make(pq.StringArray, len(params.ByMode))
		for i, m := range params.ByMode {
			byMode[i] = string(m)
		}
	}
	if params.ByStatus != nil {
		byStatus = make(pq.StringArray, len(params.ByStatus))
		for i, s := range params.ByStatus {
			byStatus[i] = string(s)
		}
	}

	rs, err := d.txOrDb(optionalTx).QueryContext(
		ctx,
		`SELECT e.project_id, e.env_id, e.id, d.id, d.mode, d.created_at, d.created_by, d.completed_at, d.status, d.status_message, d.runner_id, d.metrics, d.revision, d.rollback_to_id FROM deployment_environments e INNER JOIN deployments d ON d.de_id = e.id
		 WHERE e.org_id = $1 AND d.created_at < $2 AND ($4::text IS NULL OR e.project_id = $4) AND ($5::text IS NULL OR e.env_id = $5) AND ($6::text[] IS NULL OR d.mode = ANY($6)) AND ($7::text[] IS NULL OR d.status = ANY($7))
		 ORDER BY d.created_at DESC LIMIT $3`,
		orgId, beforeCreatedAt, limitPlusOne, params.ProjectId, params.EnvId, byMode, byStatus,
	)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to query rows")
	}
	defer func() {
		if err := rs.Close(); err != nil {
			logger.Error("failed to close row set")
		}
	}()

	out := make([]DeploymentSummary, 0, limitPlusOne-1)
	for rs.Next() {
		next := DeploymentSummary{OrgId: orgId}
		if err := rs.Scan(&next.ProjectId, &next.EnvId, &next.DeploymentEnvUuid, &next.Id, &next.Mode, &next.CreatedAt, &next.CreatedBy, opt.Scan(&next.CompletedAt), &next.Status, &next.StatusMessage, &next.RunnerId, asJson(&next.Metrics), &next.Revision, opt.Scan(&next.RollbackToId)); err != nil {
			return nil, "", errors.Wrap(err, "failed to scan row")
		}
		if len(out) >= limitPlusOne-1 {
			last := out[len(out)-1]
			return out, strconv.Itoa(int(last.CreatedAt.UnixNano())), nil
		}
		out = append(out, next)
	}

	if rs.Err() != nil {
		return nil, "", errors.Wrap(rs.Err(), "failed to iterate rows")
	}

	return out, "", nil
}

func (d *databaser) ListLastDeployments(ctx context.Context, optionalTx Tx, orgId string, pageToken string, perPage int, params ListLastDeploymentsParams) ([]DeploymentSummary, string, error) {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)

	var afterProjectId, afterEnvId string
	if pageToken != "" {
		parts := strings.Split(pageToken, ",")
		if len(parts) <= 1 {
			return nil, "", NewErrBadRequest(fmt.Sprintf("invalid page token %q", pageToken))
		}
		afterProjectId, afterEnvId = parts[0], parts[1]
	}
	limitPlusOne := max(1, perPage) + 1

	if rs, err := d.txOrDb(optionalTx).QueryContext(
		ctx,
		`SELECT e.project_id, e.env_id, e.id, d.id, d.mode, d.created_at, d.created_by, d.completed_at, d.status, d.status_message, d.runner_id, d.metrics, d.revision, d.rollback_to_id FROM deployment_environments e INNER JOIN deployments d ON d.de_id = e.id AND 
		(CASE WHEN $7 THEN d.id = e.last_state_deployment_id ELSE d.id = e.last_executing_deployment_id END)
		 WHERE e.org_id = $1 AND (e.project_id, e.env_id) > ($2, $3) AND ($5::text IS NULL OR e.project_id = $5) AND ($6::text IS NULL OR e.env_id = $6)
		 ORDER BY e.org_id, e.project_id, e.env_id LIMIT $4`,
		orgId, afterProjectId, afterEnvId, limitPlusOne, params.ProjectId, params.EnvId, params.StateChangeOnly,
	); err != nil {
		return nil, "", errors.Wrap(err, "failed to query rows")
	} else {
		defer func() {
			if err := rs.Close(); err != nil {
				logger.Error("failed to close row set")
			}
		}()
		out := make([]DeploymentSummary, 0, limitPlusOne-1)
		for rs.Next() {
			next := DeploymentSummary{OrgId: orgId}
			if err := rs.Scan(&next.ProjectId, &next.EnvId, &next.DeploymentEnvUuid, &next.Id, &next.Mode, &next.CreatedAt, &next.CreatedBy, opt.Scan(&next.CompletedAt), &next.Status, &next.StatusMessage, &next.RunnerId, asJson(&next.Metrics),
				&next.Revision, opt.Scan(&next.RollbackToId)); err != nil {
				return nil, "", errors.Wrap(err, "failed to scan row")
			}
			if len(out) >= limitPlusOne-1 {
				last := out[len(out)-1]
				return out, fmt.Sprintf("%s,%s", last.ProjectId, last.EnvId), nil
			}
			out = append(out, next)
		}
		if rs.Err() != nil {
			return nil, "", errors.Wrap(rs.Err(), "failed to iterate rows")
		}
		return out, "", nil
	}
}

func (d *databaser) GetDeploymentByIdempotencyKeyDigest(ctx context.Context, optionalTx Tx, orgId, projectId, envId, idempotencyKeyDigest string) (*DeploymentSummary, EncodedDeploymentManifest, error) {
	if optionalTx != nil {
		// A row lock cannot serialize the first use of a key, before a row exists.
		// Keep the key locked through the caller's create/replay transaction.
		identity, err := json.Marshal([]string{"deployment-command", orgId, projectId, envId, idempotencyKeyDigest})
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to encode deployment command lock")
		}
		if _, err := optionalTx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", string(identity)); err != nil {
			return nil, nil, errors.Wrap(err, "failed to lock deployment command")
		}
	}
	out := &DeploymentSummary{
		OrgId: orgId,
	}
	var rawManifest json.RawMessage
	if err := d.txOrDb(optionalTx).QueryRowContext(
		ctx,
		`SELECT e.project_id, e.env_id, e.id, d.id, d.created_at, d.created_by, d.completed_at, d.mode, d.status, d.status_message, d.manifest, d.runner_id, d.metrics, d.revision, d.rollback_to_id, d.module_version_request_digest FROM deployments d INNER JOIN deployment_environments e ON d.de_id = e.id
		WHERE e.org_id = $1 AND e.project_id = $2 AND e.env_id = $3 AND d.idempotency_key_digest = $4 AND d.created_at > $5 FOR UPDATE`,
		orgId, projectId, envId, idempotencyKeyDigest, time.Now().UTC().Add(-idempotencyDigestTimeLimit),
	).
		Scan(&out.ProjectId, &out.EnvId, &out.DeploymentEnvUuid, &out.Id, &out.CreatedAt, &out.CreatedBy, opt.Scan(&out.CompletedAt), &out.Mode, &out.Status, &out.StatusMessage, &rawManifest, &out.RunnerId, asJson(&out.Metrics), &out.Revision, opt.Scan(&out.RollbackToId), &out.ModuleVersionRequestDigest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, NewErrNotFound("no deployment found with the given idempotency key digest")
		}
		return nil, nil, errors.Wrap(err, "failed to query for deployment")
	}
	return out, EncodedDeploymentManifest(rawManifest), nil
}

func (d *databaser) GetDeployment(ctx context.Context, optionalTx Tx, orgId string, deploymentId uuid.UUID, mode GetMode) (*DeploymentSummary, EncodedDeploymentManifest, RawTofu, EncodedDeploymentGraph, error) {
	out := &DeploymentSummary{
		OrgId: orgId,
		Id:    deploymentId,
	}
	var rawManifest json.RawMessage
	var tofu []byte
	var graph json.RawMessage
	query := `SELECT e.project_id, e.env_id, e.id, d.created_at, d.created_by, d.completed_at, d.mode, d.status, d.status_message, d.manifest, d.tofu, d.runner_id, d.graph, d.metrics, d.encrypted_outputs_recipient, d.encrypted_logs_recipient, d.revision, d.runner_log_level, d.rollback_to_id, d.module_artifacts, d.module_version_request_digest
	FROM deployments d INNER JOIN deployment_environments e ON d.de_id = e.id WHERE e.org_id = $1 AND d.id = $2`
	query += GetModeSuffix(mode)
	if err := d.txOrDb(optionalTx).QueryRowContext(
		ctx,
		query,
		orgId, deploymentId,
	).Scan(&out.ProjectId, &out.EnvId, &out.DeploymentEnvUuid, &out.CreatedAt, &out.CreatedBy, opt.Scan(&out.CompletedAt), &out.Mode, &out.Status, &out.StatusMessage, &rawManifest, &tofu, &out.RunnerId, &graph, asJson(&out.Metrics),
		opt.Scan(&out.EncryptedOutputsRecipient), opt.Scan(&out.EncryptedLogsRecipient), &out.Revision, &out.RunnerLogLevel, opt.Scan(&out.RollbackToId), asJson(&out.ModuleArtifacts), &out.ModuleVersionRequestDigest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil, nil, NewErrNotFound(fmt.Sprintf("deployment '%s' not found", deploymentId))
		}
		return nil, nil, nil, nil, errors.Wrap(err, "failed to query for deployment")
	}
	return out, EncodedDeploymentManifest(rawManifest), tofu, EncodedDeploymentGraph(graph), nil
}

func (d *databaser) DeleteDeploymentsForEnv(ctx context.Context, tx Tx, orgId, projectId, envId string, force bool) error {
	if tx == nil {
		return errors.New("transaction is required")
	}

	if !force {
		var id uuid.UUID
		if err := tx.QueryRowContext(ctx, `SELECT d.id FROM deployment_environments e INNER JOIN deployments d ON d.de_id = e.id AND d.id = e.last_executing_deployment_id WHERE e.org_id = $1 AND e.project_id = $2 AND e.env_id = $3 AND d.status = 'executing' FOR UPDATE`,
			orgId, projectId, envId).Scan(&id); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return errors.Wrap(err, "failed to query for existing executing deployment")
			}
		} else {
			return NewErrConflict(fmt.Sprintf("deployment '%s' is still executing", id))
		}
	}

	if rs, err := tx.ExecContext(ctx, `DELETE FROM deployment_environments WHERE org_id = $1 AND project_id = $2 AND env_id = $3`, orgId, projectId, envId); err != nil {
		return errors.Wrap(err, "failed to delete deployment environments")
	} else if _, err := rs.RowsAffected(); err != nil {
		return errors.Wrap(err, "failed to get rows affected")
	} else {
		return nil
	}
}

func (d *databaser) GetLastDeployment(ctx context.Context, optionalTx Tx, orgId string, projectId, envId string, params GetLastDeploymentParams) (*DeploymentSummary, EncodedDeploymentManifest, RawTofu, EncodedDeploymentGraph, error) {
	out := &DeploymentSummary{
		OrgId:     orgId,
		ProjectId: projectId,
		EnvId:     envId,
	}
	var rawManifest json.RawMessage
	var rawGraph json.RawMessage
	var rawTofu []byte
	if err := d.txOrDb(optionalTx).QueryRowContext(
		ctx,
		`SELECT d.id, e.id, d.created_at, d.created_by, d.completed_at, d.mode, d.status, d.status_message, d.manifest, d.graph, d.tofu, d.runner_id, d.metrics, d.revision, d.rollback_to_id
			FROM deployments d INNER JOIN deployment_environments e ON d.de_id = e.id AND (CASE WHEN $4 THEN d.id = e.last_state_deployment_id ELSE d.id = e.last_executing_deployment_id END)
        	WHERE e.org_id = $1 AND e.project_id = $2 AND e.env_id = $3`,
		orgId, projectId, envId, params.StateChangeOnly,
	).Scan(&out.Id, &out.DeploymentEnvUuid, &out.CreatedAt, &out.CreatedBy, opt.Scan(&out.CompletedAt), &out.Mode, &out.Status, &out.StatusMessage, &rawManifest, &rawGraph, &rawTofu, &out.RunnerId, asJson(&out.Metrics), &out.Revision, opt.Scan(&out.RollbackToId)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, nil, nil, NewErrNotFound(fmt.Sprintf("no deployments found in env '%s' of project '%s'", envId, projectId))
		}
		return nil, nil, nil, nil, errors.Wrap(err, "failed to query for deployment")
	}
	return out, EncodedDeploymentManifest(rawManifest), rawTofu, EncodedDeploymentGraph(rawGraph), nil
}

func (d *databaser) GetEnvironmentDeploymentReadiness(ctx context.Context, optionalTx Tx, orgID string, refs []EnvironmentRef) ([]EnvironmentDeploymentReadiness, error) {
	if len(refs) == 0 {
		return []EnvironmentDeploymentReadiness{}, nil
	}
	projectIDs := make([]string, len(refs))
	environmentIDs := make([]string, len(refs))
	for index, ref := range refs {
		projectIDs[index] = ref.ProjectID
		environmentIDs[index] = ref.EnvID
	}
	rows, err := d.txOrDb(optionalTx).QueryContext(ctx, `
		WITH requested AS (
			SELECT project_id, env_id, ordinal
			FROM unnest($2::text[], $3::text[]) WITH ORDINALITY AS request(project_id, env_id, ordinal)
		)
		SELECT request.project_id, request.env_id, environment.id,
		       latest.id, latest.status, latest.mode, lease.expires_at,
		       base.id, COALESCE(base.module_artifacts, '[]'::jsonb)
		FROM requested request
		LEFT JOIN deployment_environments environment
		  ON environment.org_id=$1 AND environment.project_id=request.project_id AND environment.env_id=request.env_id
		LEFT JOIN deployments latest ON latest.id=environment.last_state_deployment_id
		LEFT JOIN environment_deployment_leases lease
		  ON lease.environment_id=environment.id AND lease.deployment_id=latest.id AND lease.expires_at > now()
		LEFT JOIN LATERAL (
			SELECT deployment.id, deployment.module_artifacts
			FROM deployments deployment
			WHERE deployment.de_id=environment.id
			  AND deployment.status='succeeded'
			  AND deployment.mode IN ('deploy', 'rollback', 'destroy')
			ORDER BY deployment.created_at DESC
			LIMIT 1
		) base ON true
		ORDER BY request.ordinal`, orgID, pq.Array(projectIDs), pq.Array(environmentIDs))
	if err != nil {
		return nil, errors.Wrap(err, "failed to query environment deployment readiness")
	}
	defer func() { _ = rows.Close() }()
	result := make([]EnvironmentDeploymentReadiness, 0, len(refs))
	for rows.Next() {
		var item EnvironmentDeploymentReadiness
		var artifacts json.RawMessage
		if err := rows.Scan(
			&item.ProjectID, &item.EnvID, opt.Scan(&item.EnvironmentUUID),
			opt.Scan(&item.LatestDeploymentID), opt.Scan(&item.LatestStatus), opt.Scan(&item.LatestMode),
			opt.Scan(&item.ActiveLeaseUntil), opt.Scan(&item.EffectiveBaseDeploymentID), &artifacts,
		); err != nil {
			return nil, errors.Wrap(err, "failed to scan environment deployment readiness")
		}
		if err := json.Unmarshal(artifacts, &item.ActiveModules); err != nil {
			return nil, errors.Wrap(err, "failed to decode active module inventory")
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to iterate environment deployment readiness")
	}
	return result, nil
}

func (d *databaser) UpdateDeploymentStatusAndOutputs(ctx context.Context, optionalTx Tx, deploymentId uuid.UUID, params UpdateDeploymentStatusAndOutputsParams) (*DeploymentSummary, error) {
	out := &DeploymentSummary{
		Id: deploymentId,
	}
	if err := d.txOrDb(optionalTx).QueryRowContext(
		ctx,
		`UPDATE deployments d
			SET status = $2, status_message = $3, completed_at = $4, outputs = $5, metrics = $6, revision = revision + 1
			FROM deployment_environments e
			WHERE d.de_id = e.id AND d.id = $1 AND ($7::integer IS NULL OR d.revision = $7)
			RETURNING e.org_id, e.project_id, e.env_id, e.id, d.mode, d.created_at, d.completed_at, d.status, d.status_message, d.runner_id, d.metrics, d.revision, d.rollback_to_id
			`,
		deploymentId, params.Status, params.StatusMessage, time.Now().UTC(), params.Outputs, asJson(&params.Metrics), params.ExpectedRevision,
	).Scan(&out.OrgId, &out.ProjectId, &out.EnvId, &out.DeploymentEnvUuid, &out.Mode, &out.CreatedAt, opt.Scan(&out.CompletedAt), &out.Status, &out.StatusMessage, &out.RunnerId, asJson(&out.Metrics), &out.Revision, opt.Scan(&out.RollbackToId)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if params.ExpectedRevision.IsSet() {
				return nil, NewErrConflict("deployment not found at revision")
			}
			return nil, NewErrNotFound("deployment not found")
		}
		return nil, errors.Wrap(err, "failed to update deployment")
	}
	if params.Status == DeploymentStatusSucceeded || params.Status == DeploymentStatusFailed {
		if params.RunnerCompletionProven {
			if err := RecordDeploymentCompletion(ctx, d.txOrDb(optionalTx), deploymentId); err != nil {
				return nil, err
			}
		}
		if _, err := d.txOrDb(optionalTx).ExecContext(ctx, `DELETE FROM environment_deployment_leases WHERE deployment_id = $1`, deploymentId); err != nil {
			return nil, errors.Wrap(err, "failed to release environment deployment lease")
		}
	}
	return out, nil
}

func CompleteDeploymentSnapshot(manifest EncodedDeploymentManifest, tofu RawTofu, graph EncodedDeploymentGraph) bool {
	return len(tofu) > 0 && json.Valid(manifest) && json.Valid(graph) && strings.HasPrefix(strings.TrimSpace(string(manifest)), "{") && strings.HasPrefix(strings.TrimSpace(string(graph)), "{")
}

func RequiresEnvironmentLease(mode DeploymentMode) bool {
	return mode == DeploymentModeDeploy || mode == DeploymentModeRollback || mode == DeploymentModeDestroy
}

func (d *databaser) RenewDeploymentLease(ctx context.Context, optionalTx Tx, deploymentID uuid.UUID, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = DefaultEnvironmentLeaseTTL
	}
	result, err := d.txOrDb(optionalTx).ExecContext(ctx, `UPDATE environment_deployment_leases lease
		SET heartbeat_at=now(), expires_at=now() + $1::interval
		FROM deployments deployment
		WHERE lease.deployment_id=$2 AND deployment.id=lease.deployment_id AND deployment.status='executing'`, ttl.String(), deploymentID)
	if err != nil {
		return errors.Wrap(err, "failed to renew environment deployment lease")
	}
	count, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "failed to read renewed lease count")
	}
	if count != 1 {
		return NewErrConflict("deployment no longer owns its environment lease")
	}
	return nil
}

func (d *databaser) CreateDeploymentHistoryRecord(ctx context.Context, optionalTx Tx, dep *DeploymentSummary) error {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)
	if _, err := d.txOrDb(optionalTx).ExecContext(
		ctx,
		`INSERT INTO deployment_history (id, org_id, project_id, env_id, env_uuid, mode, created_by, created_at, completed_at, status, metrics)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11);
`,
		dep.Id, dep.OrgId, dep.ProjectId, dep.EnvId, dep.DeploymentEnvUuid, dep.Mode, dep.CreatedBy, dep.CreatedAt, dep.CompletedAt.Must(), dep.Status, asJson(&dep.Metrics),
	); err != nil {
		return errors.Wrap(err, "failed to insert row")
	}
	logger.Info("inserted deployment history record")
	return nil
}

func (d *databaser) DeleteDeploymentOutputsByCompletionDate(ctx context.Context, optionalTx Tx, completedBefore time.Time) ([]DeploymentSummary, error) {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)
	// DP-owned external material follows its retained Deployment, not the short
	// user-key transfer window. A replay receipt alone is insufficient: it may
	// outlive the Deployment and must match the exact scoped wrapped identity.
	if rs, err := d.txOrDb(optionalTx).QueryContext(
		ctx, `
WITH UpdatedLateResults AS (
    UPDATE deployment_late_runner_results late SET encrypted_outputs=NULL
    FROM deployments d JOIN deployment_environments environment ON d.de_id=environment.id
    WHERE late.deployment_id=d.id AND d.completed_at<$1 AND late.encrypted_outputs IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM external_operation_deployments receipt
          JOIN external_operation_material material
            ON material.org_id=receipt.org_id AND material.plugin_id=receipt.plugin_id
           AND material.operation_ref::text=receipt.operation_ref
          WHERE receipt.deployment_id=d.id AND receipt.org_id=environment.org_id
            AND receipt.project_id=environment.project_id AND receipt.env_id=environment.env_id
            AND receipt.environment_uuid=environment.id AND receipt.project_uuid IS NOT NULL
            AND material.recipient=d.encrypted_outputs_recipient AND material.recipient=late.recipient
            AND material.encrypted_identity ~ '^vault:v[1-9][0-9]*:.+$'
      )
), UpdatedDeployments AS (
    UPDATE deployments d
    SET outputs = ''::bytea
    FROM deployment_environments environment
    WHERE d.de_id = environment.id AND d.completed_at < $1 AND d.outputs != ''::bytea
      AND NOT EXISTS (
          SELECT 1 FROM external_operation_deployments receipt
          JOIN external_operation_material material
            ON material.org_id = receipt.org_id AND material.plugin_id = receipt.plugin_id
           AND material.operation_ref::text = receipt.operation_ref
          WHERE receipt.deployment_id = d.id AND receipt.org_id = environment.org_id
            AND receipt.project_id = environment.project_id AND receipt.env_id = environment.env_id
            AND receipt.environment_uuid = environment.id AND receipt.project_uuid IS NOT NULL
            AND material.recipient = d.encrypted_outputs_recipient
            AND material.encrypted_identity ~ '^vault:v[1-9][0-9]*:.+$'
      )
    RETURNING d.de_id, d.id
)
SELECT ud.id, e.org_id, e.project_id, e.env_id
FROM UpdatedDeployments ud
INNER JOIN deployment_environments e ON ud.de_id = e.id;`, completedBefore,
	); err != nil {
		return nil, errors.Wrap(err, "failed to query rows")
	} else {
		defer func() {
			if err := rs.Close(); err != nil {
				logger.Error("failed to close row set")
			}
		}()
		out := make([]DeploymentSummary, 0)
		for rs.Next() {
			next := DeploymentSummary{}
			if err := rs.Scan(&next.Id, &next.OrgId, &next.ProjectId, &next.EnvId); err != nil {
				return nil, errors.Wrap(err, "failed to scan row")
			}
			out = append(out, next)
		}
		if rs.Err() != nil {
			return nil, errors.Wrap(rs.Err(), "failed to iterate rows")
		}
		return out, nil
	}
}

func (d *databaser) GetDeploymentEncryptedOutputs(ctx context.Context, optionalTx Tx, orgId string, deploymentId uuid.UUID) (string, error) {
	var out string
	if err := d.txOrDb(optionalTx).QueryRowContext(ctx, `SELECT COALESCE(NULLIF(d.outputs,''::bytea),
        (SELECT late.encrypted_outputs FROM deployment_late_runner_results late WHERE late.deployment_id=d.id
         AND late.org_id=e.org_id AND late.runner_id=d.runner_id AND late.result_status='success' AND late.recipient=d.encrypted_outputs_recipient),''::bytea)
        FROM deployments d INNER JOIN deployment_environments e ON d.de_id = e.id WHERE e.org_id = $1 AND d.id = $2`, orgId, deploymentId).Scan(&out); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", NewErrNotFound(fmt.Sprintf("deployment '%s' not found", deploymentId))
		}
		return "", errors.Wrap(err, "failed to query for deployment")
	} else {
		return out, nil
	}
}

type ListLastDeploymentsByNodePropertiesParams struct {
	ModuleId      opt.Opt[string]
	ModuleVersion opt.Opt[string]
	// NOTE: expand here with other filters in the future like module version, resource type, class, id, etc..
}

func (d *databaser) ListLastDeploymentsByNodeProperties(ctx context.Context, optionalTx Tx, orgId string, pageToken string, perPage int, params ListLastDeploymentsByNodePropertiesParams) ([]DeploymentSummary, string, error) {
	logger := hlogger.TraceScopedLoggerFromCtx(d.logger, ctx)

	var afterProjectId, afterEnvId string
	if pageToken != "" {
		parts := strings.Split(pageToken, ",")
		if len(parts) <= 1 {
			return nil, "", NewErrBadRequest(fmt.Sprintf("invalid page token %q", pageToken))
		}
		afterProjectId, afterEnvId = parts[0], parts[1]
	}
	limitPlusOne := max(1, perPage) + 1

	if rs, err := d.txOrDb(optionalTx).QueryContext(
		ctx,
		`SELECT e.project_id, e.env_id, e.id, d.id, d.mode, d.created_at, d.created_by, d.completed_at, d.status, d.status_message, d.runner_id, d.metrics, d.revision, d.rollback_to_id FROM deployment_environments e
    	 INNER JOIN deployments d ON d.de_id = e.id AND d.id = e.last_state_deployment_id
		 WHERE e.org_id = $1 AND (e.project_id, e.env_id) > ($2, $3) AND EXISTS (
		 	SELECT 1 FROM resource_nodes n WHERE n.env_uuid = e.id AND ($5::text IS NULL OR n.last_module_definition_id = $5) AND ($6::text IS NULL OR n.last_module_definition_version = $6)
	   	 )
		 ORDER BY e.org_id, e.project_id, e.env_id LIMIT $4`,
		orgId, afterProjectId, afterEnvId, limitPlusOne, params.ModuleId, params.ModuleVersion,
	); err != nil {
		return nil, "", errors.Wrap(err, "failed to query rows")
	} else {
		defer func() {
			if err := rs.Close(); err != nil {
				logger.Error("failed to close row set")
			}
		}()
		out := make([]DeploymentSummary, 0, limitPlusOne-1)
		for rs.Next() {
			next := DeploymentSummary{OrgId: orgId}
			if err := rs.Scan(&next.ProjectId, &next.EnvId, &next.DeploymentEnvUuid, &next.Id, &next.Mode, &next.CreatedAt, &next.CreatedBy, opt.Scan(&next.CompletedAt), &next.Status, &next.StatusMessage, &next.RunnerId,
				asJson(&next.Metrics), &next.Revision, opt.Scan(&next.RollbackToId)); err != nil {
				return nil, "", errors.Wrap(err, "failed to scan row")
			}
			if len(out) >= limitPlusOne-1 {
				last := out[len(out)-1]
				return out, fmt.Sprintf("%s,%s", last.ProjectId, last.EnvId), nil
			}
			out = append(out, next)
		}
		if rs.Err() != nil {
			return nil, "", errors.Wrap(rs.Err(), "failed to iterate rows")
		}
		return out, "", nil
	}
}
