package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// RetainedPlanBinding is the independently supplied execution target identity.
type RetainedPlanBinding struct {
	Schema             string `json:"schema"`
	OrgID              string `json:"org_id"`
	EnvironmentID      string `json:"environment_id"`
	RunnerID           string `json:"runner_id"`
	SourceDeploymentID string `json:"source_deployment_id"`
	BaseDeploymentID   string `json:"base_deployment_id"`
	Backend            string `json:"backend"`
	Tool               string `json:"tool"`
}

type RetainedPlan struct {
	ID                                                             uuid.UUID
	OrgID, ProjectID, EnvID                                        string
	EnvironmentID, CreatorID, SourceDeploymentID, BaseDeploymentID uuid.UUID
	RunnerID                                                       string
	Binding                                                        RetainedPlanBinding
	Recipient, EncryptedIdentity                                   string
	RequestDigest                                                  string
	Lifecycle                                                      json.RawMessage
	Ciphertext                                                     []byte
	ApplyOutputs                                                   []byte
	CiphertextSHA256, Subject, ApprovalSubject, ApplyKeyDigest     sql.NullString
	ApplyDeploymentID                                              uuid.NullUUID
	ExpiresAt                                                      time.Time
}

func InsertRetainedPlan(ctx context.Context, tx Tx, p RetainedPlan) error {
	binding, err := json.Marshal(p.Binding)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO retained_plans
       (id,org_id,project_id,env_id,environment_id,creator_id,source_deployment_id,base_deployment_id,runner_id,binding,recipient,encrypted_identity,lifecycle,request_digest)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, p.ID, p.OrgID, p.ProjectID, p.EnvID, p.EnvironmentID, p.CreatorID, p.SourceDeploymentID, p.BaseDeploymentID, p.RunnerID, binding, p.Recipient, p.EncryptedIdentity, p.Lifecycle, p.RequestDigest)
	return err
}

func ReadRetainedPlan(ctx context.Context, tx Tx, org string, id uuid.UUID, lock bool) (*RetainedPlan, error) {
	query := `SELECT id,org_id,project_id,env_id,environment_id,creator_id,source_deployment_id,base_deployment_id,runner_id,binding,recipient,encrypted_identity,lifecycle,request_digest,ciphertext,apply_outputs,ciphertext_sha256,subject,approval_subject,apply_deployment_id,apply_key_digest,expires_at FROM retained_plans WHERE org_id=$1 AND id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	var p RetainedPlan
	var binding []byte
	err := tx.QueryRowContext(ctx, query, org, id).
		Scan(&p.ID, &p.OrgID, &p.ProjectID, &p.EnvID, &p.EnvironmentID, &p.CreatorID, &p.SourceDeploymentID, &p.BaseDeploymentID, &p.RunnerID, &binding, &p.Recipient, &p.EncryptedIdentity, &p.Lifecycle, &p.RequestDigest, &p.Ciphertext, &p.ApplyOutputs, &p.CiphertextSHA256, &p.Subject, &p.ApprovalSubject, &p.ApplyDeploymentID, &p.ApplyKeyDigest, &p.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, NewErrNotFound("retained plan not found")
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(binding, &p.Binding); err != nil {
		return nil, err
	}
	return &p, nil
}

type RetainedPlanDispatch struct {
	Export           bool
	RequesterID      uuid.UUID
	Binding          RetainedPlanBinding
	Ciphertext       []byte
	CiphertextSHA256 string
}

// LoadRetainedPlanDispatch supplies internal-only Runner configuration.
func LoadRetainedPlanDispatch(ctx context.Context, tx Tx, org string, deployment uuid.UUID) (*RetainedPlanDispatch, error) {
	var binding []byte
	var data []byte
	var digest sql.NullString
	var export bool
	var requester uuid.UUID
	err := tx.QueryRowContext(ctx, `SELECT binding, ciphertext, ciphertext_sha256, source_deployment_id=$2, creator_id
      FROM retained_plans WHERE org_id=$1 AND (source_deployment_id=$2 OR apply_deployment_id=$2)`, org, deployment).Scan(&binding, &data, &digest, &export, &requester)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out RetainedPlanDispatch
	if err := json.Unmarshal(binding, &out.Binding); err != nil {
		return nil, err
	}
	out.Export = export
	out.RequesterID = requester
	out.Ciphertext = data
	out.CiphertextSHA256 = digest.String
	if !export && (len(data) == 0 || !digest.Valid) {
		return nil, errors.New("retained plan material is unavailable")
	}
	return &out, nil
}
