package model

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrSystemMaintenance = errors.New("the installation is in maintenance mode. Existing deployments continue; new requests are not accepted")
var ErrSettingsRevision = errors.New("installation settings changed; reload before saving")

type InstallationMaintenance struct {
	Enabled      bool       `json:"enabled"`
	Revision     int64      `json:"revision"`
	UpdatedAt    time.Time  `json:"updated_at"`
	UpdatedBy    *uuid.UUID `json:"updated_by"`
	Executing    int64      `json:"executing_deployments"`
	Queued       int64      `json:"queued_deployments"`
	Runners      int64      `json:"runner_reservations"`
	Admissions   int64      `json:"unsettled_admissions"`
	Observations int64      `json:"executing_observations"`
}

func ReadInstallationMaintenance(ctx context.Context, db Databaser) (InstallationMaintenance, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return InstallationMaintenance{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var out InstallationMaintenance
	err = tx.QueryRowContext(ctx, `SELECT enabled,revision,updated_at,updated_by,
      (SELECT count(*) FROM deployments WHERE status='executing'),
      (SELECT count(*) FROM environment_deployment_queue WHERE state='queued'),
      (SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL),
      (SELECT count(*) FROM deployment_admission_receipts WHERE core_settled_at IS NULL),
      (SELECT count(*) FROM deployments WHERE status='executing' AND mode='observation')
      FROM installation_maintenance WHERE singleton`).Scan(&out.Enabled, &out.Revision, &out.UpdatedAt, &out.UpdatedBy, &out.Executing, &out.Queued, &out.Runners, &out.Admissions, &out.Observations)
	return out, err
}

func UpdateInstallationMaintenance(ctx context.Context, db Databaser, actor uuid.UUID, revision int64, enabled bool) (InstallationMaintenance, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return InstallationMaintenance{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var current int64
	if err := tx.QueryRowContext(ctx, `UPDATE installation_maintenance SET enabled=$1,revision=revision+1,updated_at=statement_timestamp(),updated_by=$2 WHERE singleton AND revision=$3 RETURNING revision`, enabled, actor, revision).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return InstallationMaintenance{}, ErrSettingsRevision
		}
		return InstallationMaintenance{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO installation_maintenance_events(revision,enabled,changed_by) VALUES($1,$2,$3)`, current, enabled, actor); err != nil {
		return InstallationMaintenance{}, err
	}
	if err := tx.Commit(); err != nil {
		return InstallationMaintenance{}, err
	}
	return ReadInstallationMaintenance(ctx, db)
}

// The shared row lock lasts until the caller's acceptance transaction commits.
// A maintenance UPDATE waits for earlier acceptance and fences later acceptance.
// It never locks queue/deployment rows, so it cannot invert their lock order.
func requireDeploymentAcceptance(ctx context.Context, tx Tx, queueID uuid.UUID, org, project, env string, environmentID, actor uuid.UUID, mode DeploymentMode) error {
	var enabled bool
	if err := tx.QueryRowContext(ctx, `SELECT enabled FROM installation_maintenance WHERE singleton FOR SHARE`).Scan(&enabled); err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	if queueID != uuid.Nil {
		var accepted bool
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM environment_deployment_queue WHERE id=$1 AND org_id=$2 AND project_id=$3 AND env_id=$4 AND environment_uuid=$5 AND created_by=$6 AND mode=$7 AND state='queued' AND expires_at>statement_timestamp())`, queueID, org, project, env, environmentID, actor, mode).
			Scan(&accepted)
		if err != nil {
			return err
		}
		if accepted {
			return nil
		}
	}
	return ErrSystemMaintenance
}
