package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
)

const PipelineControlVersion = "stellwerk.pipeline.control/v1"
const PipelineControlResultVersion = "stellwerk.pipeline.control-result/v1"
const PipelineControlCommandType = "control-rollout-v1"
const PipelineControlEventType = "rollout-control-result"
const PipelineControlOutboxSubject = "io.platform-orchestrator.pipeline.control.requested"
const PipelineControlMaxBytes = 8 << 10

// These DTOs mirror the Runner wire contract; DP cannot import Runner internal
// packages. The original execute and control use the same signing key.
type PipelineControlRequest struct {
	Version         string                            `json:"version"`
	ID              string                            `json:"id"`
	RolloutID       string                            `json:"rollout_id"`
	OrgID           string                            `json:"org_id"`
	EnvironmentID   string                            `json:"environment_id"`
	Target          cp.NativeExecutionTargetReference `json:"target"`
	ExecutionDigest string                            `json:"execution_digest"`
	ActorID         string                            `json:"actor_id"`
	Action          string                            `json:"action"`
	Subject         string                            `json:"subject"`
	FailedStep      string                            `json:"failed_step"`
	Revision        int64                             `json:"revision"`
	IssuedAt        time.Time                         `json:"issued_at"`
	ExpiresAt       time.Time                         `json:"expires_at"`
}

type PipelineControlResult struct {
	Version         string `json:"version"`
	ControlID       string `json:"control_id"`
	RolloutID       string `json:"rollout_id"`
	RequestDigest   string `json:"request_digest"`
	ExecutionDigest string `json:"execution_digest"`
	Outcome         string `json:"outcome"`
	Code            string `json:"code"`
	Phase           string `json:"phase"`
	Revision        int64  `json:"revision"`
}

type PipelineControl struct {
	ID                uuid.UUID       `json:"id"`
	DeploymentID      uuid.UUID       `json:"deployment_id"`
	EnvironmentID     uuid.UUID       `json:"-"`
	RunnerUUID        uuid.UUID       `json:"-"`
	OrgID             string          `json:"-"`
	ActorID           uuid.UUID       `json:"actor_id"`
	ClientKey         string          `json:"-"`
	IntentDigest      string          `json:"-"`
	RequestDigest     string          `json:"-"`
	ExecutionDigest   string          `json:"-"`
	Action            string          `json:"action"`
	Command           []byte          `json:"-"`
	CreatedAt         time.Time       `json:"created_at"`
	ExpiresAt         time.Time       `json:"expires_at"`
	Result            json.RawMessage `json:"result,omitempty"`
	ResultEventDigest sql.NullString  `json:"-"`
	ResolvedAt        sql.NullTime    `json:"-"`
}

const pipelineControlColumns = `id,org_id,deployment_id,environment_id,runner_uuid,actor_id,client_key,intent_digest,request_digest,execution_digest,action,command,created_at,expires_at,result,result_event_digest,resolved_at`

func readPipelineControl(row *sql.Row) (*PipelineControl, error) {
	var r PipelineControl
	var result []byte
	err := row.Scan(&r.ID, &r.OrgID, &r.DeploymentID, &r.EnvironmentID, &r.RunnerUUID, &r.ActorID, &r.ClientKey, &r.IntentDigest, &r.RequestDigest, &r.ExecutionDigest, &r.Action, &r.Command, &r.CreatedAt, &r.ExpiresAt, &result, &r.ResultEventDigest, &r.ResolvedAt)
	r.Result = result
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound("pipeline control not found")
	}
	return &r, err
}

func ReadPipelineControl(ctx context.Context, tx Tx, org string, deployment, id uuid.UUID) (*PipelineControl, error) {
	return readPipelineControl(tx.QueryRowContext(ctx, `SELECT `+pipelineControlColumns+` FROM deployment_pipeline_controls WHERE org_id=$1 AND deployment_id=$2 AND id=$3`, org, deployment, id))
}

func FindPipelineControl(ctx context.Context, tx Tx, org string, deployment, actor uuid.UUID, key string) (*PipelineControl, error) {
	return readPipelineControl(tx.QueryRowContext(ctx, `SELECT `+pipelineControlColumns+` FROM deployment_pipeline_controls WHERE org_id=$1 AND deployment_id=$2 AND actor_id=$3 AND client_key=$4`, org, deployment, actor, key))
}

func InsertPipelineControl(ctx context.Context, tx Tx, r *PipelineControl) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO deployment_pipeline_controls
    (id,org_id,deployment_id,environment_id,runner_uuid,actor_id,client_key,intent_digest,request_digest,execution_digest,action,command,created_at,expires_at)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, r.ID, r.OrgID, r.DeploymentID, r.EnvironmentID, r.RunnerUUID, r.ActorID, r.ClientKey, r.IntentDigest, r.RequestDigest, r.ExecutionDigest, r.Action, r.Command, r.CreatedAt, r.ExpiresAt)
	return err
}

func LockPipelineControl(ctx context.Context, tx Tx, org string, deployment, id uuid.UUID) (*PipelineControl, error) {
	return readPipelineControl(tx.QueryRowContext(ctx, `SELECT `+pipelineControlColumns+` FROM deployment_pipeline_controls WHERE org_id=$1 AND deployment_id=$2 AND id=$3 FOR UPDATE`, org, deployment, id))
}
