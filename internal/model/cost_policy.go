package model

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrCostAPIKeyRequired = errors.New("an Infracost API key is required to enable cost estimation")

type CostPolicy struct {
	Enabled          bool       `json:"enabled"`
	APIKeyConfigured bool       `json:"api_key_configured"`
	Revision         int64      `json:"revision"`
	UpdatedAt        time.Time  `json:"updated_at"`
	UpdatedBy        *uuid.UUID `json:"updated_by"`
}

type DeploymentCostPolicy struct {
	Enabled        bool
	PolicyRevision int64
}

func ReadCostPolicy(ctx context.Context, db Databaser) (CostPolicy, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return CostPolicy{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var out CostPolicy
	err = tx.QueryRowContext(ctx, `SELECT enabled,api_key_ciphertext<>'',revision,updated_at,updated_by FROM installation_cost_policy WHERE singleton`).Scan(&out.Enabled, &out.APIKeyConfigured, &out.Revision, &out.UpdatedAt, &out.UpdatedBy)
	return out, err
}

// ReadCostAPIKeyCiphertext is used only when assembling a fresh dispatch. The
// credential never belongs to the persisted deployment or its policy snapshot.
func ReadCostAPIKeyCiphertext(ctx context.Context, db Databaser) (string, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	var ciphertext string
	err = tx.QueryRowContext(ctx, `SELECT api_key_ciphertext FROM installation_cost_policy WHERE singleton`).Scan(&ciphertext)
	return ciphertext, err
}

// A nil ciphertext preserves the existing key; an empty ciphertext clears it.
func UpdateCostPolicy(ctx context.Context, db Databaser, actor uuid.UUID, revision int64, enabled bool, ciphertext *string) (CostPolicy, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return CostPolicy{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var currentRevision int64
	var configured bool
	if err := tx.QueryRowContext(ctx, `SELECT revision,api_key_ciphertext<>'' FROM installation_cost_policy WHERE singleton FOR UPDATE`).Scan(&currentRevision, &configured); err != nil {
		return CostPolicy{}, err
	}
	if revision != currentRevision {
		return CostPolicy{}, ErrSettingsRevision
	}
	if ciphertext != nil {
		configured = *ciphertext != ""
	}
	if enabled && !configured {
		return CostPolicy{}, ErrCostAPIKeyRequired
	}
	var out CostPolicy
	err = tx.QueryRowContext(ctx, `UPDATE installation_cost_policy SET enabled=$1,api_key_ciphertext=COALESCE($2,api_key_ciphertext),revision=revision+1,updated_at=statement_timestamp(),updated_by=$3 WHERE singleton RETURNING enabled,api_key_ciphertext<>'',revision,updated_at,updated_by`, enabled, ciphertext, actor).
		Scan(&out.Enabled, &out.APIKeyConfigured, &out.Revision, &out.UpdatedAt, &out.UpdatedBy)
	if err != nil {
		return CostPolicy{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO installation_cost_policy_events(revision,enabled,api_key_configured,api_key_changed,changed_by) VALUES($1,$2,$3,$4,$5)`, out.Revision, enabled, configured, ciphertext != nil, actor); err != nil {
		return CostPolicy{}, err
	}
	if err := tx.Commit(); err != nil {
		return CostPolicy{}, err
	}
	return out, nil
}

func (d *databaser) FreezeDeploymentCostPolicy(ctx context.Context, dep *DeploymentSummary) error {
	return FreezeDeploymentCostPolicy(ctx, d, dep)
}

// FreezeDeploymentCostPolicy persists execution settings before any dispatch.
// Old attempts without a snapshot keep their original environment on retries.
func FreezeDeploymentCostPolicy(ctx context.Context, db Databaser, dep *DeploymentSummary) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `INSERT INTO deployment_cost_policy_snapshots(deployment_id,enabled,policy_revision)
 SELECT d.id,p.enabled,p.revision FROM deployments d JOIN deployment_dispatch_budgets b ON b.deployment_id=d.id
 CROSS JOIN installation_cost_policy p
 WHERE d.id=$1 AND d.revision=$2 AND d.status='executing' AND b.deployment_revision=$2 AND b.first_attempt_at IS NULL AND NOT b.legacy
 ON CONFLICT (deployment_id) DO NOTHING`, dep.Id, dep.Revision)
	if err != nil {
		return err
	}
	var snapshot DeploymentCostPolicy
	err = tx.QueryRowContext(ctx, `SELECT enabled,policy_revision FROM deployment_cost_policy_snapshots WHERE deployment_id=$1`, dep.Id).Scan(&snapshot.Enabled, &snapshot.PolicyRevision)
	if errors.Is(err, sql.ErrNoRows) {
		dep.CostPolicy = nil
		return nil
	}
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	dep.CostPolicy = &snapshot
	return nil
}
