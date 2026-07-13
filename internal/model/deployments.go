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

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/genevents"
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

	RunnerId                  string
	Metrics                   DeploymentMetrics
	EncryptedOutputsRecipient opt.Opt[string]
	EncryptedLogsRecipient    opt.Opt[string]
	RunnerLogLevel            string
}

type UpdateDeploymentStatusAndOutputsParams struct {
	Status        DeploymentStatus
	StatusMessage string

	Outputs opt.Opt[string]
	Metrics DeploymentMetrics

	// ExpectedRevision will reject the update if the deployment is not at the same revision number
	ExpectedRevision opt.Opt[int64]
}

type EncodedDeploymentManifest json.RawMessage
type EncodedDeploymentGraph json.RawMessage
type RawTofu []byte

type CreateDeploymentParams struct {
	CreatedBy                 uuid.UUID
	DeploymentEnvUuid         uuid.UUID
	Mode                      DeploymentMode
	Manifest                  EncodedDeploymentManifest
	RollbackToId              opt.Opt[uuid.UUID]
	Graph                     EncodedDeploymentGraph
	Tofu                      RawTofu
	IdempotencyKeyDigest      opt.Opt[string]
	RunnerId                  string
	EncryptedOutputsRecipient opt.Opt[string]
	EncryptedLogsRecipient    opt.Opt[string]
	Metrics                   DeploymentMetrics
	RunnerLogLevel            string
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

	out := &DeploymentSummary{
		OrgId:                     orgId,
		ProjectId:                 projectId,
		EnvId:                     envId,
		DeploymentEnvUuid:         params.DeploymentEnvUuid,
		Id:                        uuid.New(),
		Mode:                      params.Mode,
		RollbackToId:              params.RollbackToId,
		CreatedAt:                 time.Now().UTC(),
		CreatedBy:                 params.CreatedBy,
		Status:                    DeploymentStatusExecuting,
		StatusMessage:             "Deploying...",
		RunnerId:                  params.RunnerId,
		EncryptedOutputsRecipient: params.EncryptedOutputsRecipient,
		EncryptedLogsRecipient:    params.EncryptedLogsRecipient,
		Metrics:                   params.Metrics,
		Revision:                  1,
	}

	{
		var lastDeploymentId uuid.UUID
		var lastDeploymentStatus string
		if err := tx.QueryRowContext(
			ctx,
			`SELECT d.id, d.status FROM deployment_environments e INNER JOIN deployments d ON d.de_id = e.id AND d.id = e.last_executing_deployment_id WHERE e.org_id = $1 AND e.project_id = $2 AND e.env_id = $3 FOR UPDATE`,
			orgId, projectId, envId,
		).Scan(&lastDeploymentId, &lastDeploymentStatus); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, errors.Wrap(err, "failed to query for existing executing deployment")
			}
		} else if DeploymentStatus(lastDeploymentStatus) == DeploymentStatusExecuting {
			return nil, NewErrConflict(fmt.Sprintf("deployment '%s' is still executing", lastDeploymentId))
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
		`INSERT INTO deployments (de_id, id, created_at, created_by, mode, status, status_message, manifest, graph, tofu, runner_id, idempotency_key_digest, encrypted_outputs_recipient, encrypted_logs_recipient, metrics, runner_log_level, rollback_to_id) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) RETURNING created_at`,
		out.DeploymentEnvUuid, out.Id, out.CreatedAt, out.CreatedBy, params.Mode, DeploymentStatusExecuting, "Deploying...", params.Manifest, params.Graph, params.Tofu, params.RunnerId,
		params.IdempotencyKeyDigest.Ref(), params.EncryptedOutputsRecipient.Ref(), params.EncryptedLogsRecipient.Ref(), asJson(&params.Metrics), params.RunnerLogLevel, params.RollbackToId.Ref(),
	).Scan(&out.CreatedAt); err != nil {
		return nil, errors.Wrap(err, "failed to insert deployment")
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
	out := &DeploymentSummary{
		OrgId: orgId,
	}
	var rawManifest json.RawMessage
	if err := d.txOrDb(optionalTx).QueryRowContext(
		ctx,
		`SELECT e.project_id, e.env_id, e.id, d.id, d.created_at, d.created_by, d.completed_at, d.mode, d.status, d.status_message, d.manifest, d.runner_id, d.metrics, d.revision, d.rollback_to_id FROM deployments d INNER JOIN deployment_environments e ON d.de_id = e.id 
		WHERE e.org_id = $1 AND e.project_id = $2 AND e.env_id = $3 AND d.idempotency_key_digest = $4 AND d.created_at > $5 FOR UPDATE`,
		orgId, projectId, envId, idempotencyKeyDigest, time.Now().UTC().Add(-idempotencyDigestTimeLimit),
	).Scan(&out.ProjectId, &out.EnvId, &out.DeploymentEnvUuid, &out.Id, &out.CreatedAt, &out.CreatedBy, opt.Scan(&out.CompletedAt), &out.Mode, &out.Status, &out.StatusMessage, &rawManifest, &out.RunnerId, asJson(&out.Metrics), &out.Revision, opt.Scan(&out.RollbackToId)); err != nil {
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
	query := `SELECT e.project_id, e.env_id, e.id, d.created_at, d.created_by, d.completed_at, d.mode, d.status, d.status_message, d.manifest, d.tofu, d.runner_id, d.graph, d.metrics, d.encrypted_outputs_recipient, d.encrypted_logs_recipient, d.revision, d.runner_log_level, d.rollback_to_id 
	FROM deployments d INNER JOIN deployment_environments e ON d.de_id = e.id WHERE e.org_id = $1 AND d.id = $2`
	query += GetModeSuffix(mode)
	if err := d.txOrDb(optionalTx).QueryRowContext(
		ctx,
		query,
		orgId, deploymentId,
	).Scan(&out.ProjectId, &out.EnvId, &out.DeploymentEnvUuid, &out.CreatedAt, &out.CreatedBy, opt.Scan(&out.CompletedAt), &out.Mode, &out.Status, &out.StatusMessage, &rawManifest, &tofu, &out.RunnerId, &graph, asJson(&out.Metrics),
		opt.Scan(&out.EncryptedOutputsRecipient), opt.Scan(&out.EncryptedLogsRecipient), &out.Revision, &out.RunnerLogLevel, opt.Scan(&out.RollbackToId)); err != nil {
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
	return out, nil
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
	if rs, err := d.txOrDb(optionalTx).QueryContext(
		ctx, `
WITH UpdatedDeployments AS (
    UPDATE deployments
    SET outputs = ''::bytea
    WHERE completed_at < $1 AND outputs != ''::bytea
    RETURNING de_id, id
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
	if err := d.txOrDb(optionalTx).QueryRowContext(ctx, `SELECT d.outputs FROM deployments d INNER JOIN deployment_environments e ON d.de_id = e.id WHERE e.org_id = $1 AND d.id = $2`, orgId, deploymentId).Scan(&out); err != nil {
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
