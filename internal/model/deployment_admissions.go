package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

type DeploymentAdmissionReceipt struct {
	OrgID                           string
	AdmissionID                     uuid.UUID
	ProjectUUID                     uuid.UUID
	EnvironmentUUID                 uuid.UUID
	ActorID                         uuid.UUID
	RequestDigest                   string
	Mode                            DeploymentMode
	ModuleVersions                  map[string]uuid.UUID
	CarryForwardModuleVersions      map[string]uuid.UUID
	ConfirmedRestrictedVersionUUIDs []uuid.UUID
	CoreEpoch                       int64
	Disposition                     string
	DeploymentID                    uuid.NullUUID
	TerminalOutcome                 string
	CoreSettledAt                   sql.NullTime
	CreatedAt                       time.Time
}

func lockDeploymentAdmission(ctx context.Context, tx Tx, orgID string, admissionID uuid.UUID) error {
	if tx == nil {
		return errors.New("transaction is required")
	}
	identity, err := json.Marshal([]string{"deployment-admission", orgID, admissionID.String()})
	if err != nil {
		return errors.Wrap(err, "failed to encode Deployment admission lock")
	}
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", string(identity)); err != nil {
		return errors.Wrap(err, "failed to lock Deployment admission")
	}
	return nil
}

func GetDeploymentAdmissionReceipt(ctx context.Context, tx Tx, orgID string, admissionID uuid.UUID) (*DeploymentAdmissionReceipt, error) {
	if err := lockDeploymentAdmission(ctx, tx, orgID, admissionID); err != nil {
		return nil, err
	}
	return readDeploymentAdmissionReceipt(ctx, tx, orgID, admissionID, uuid.Nil)
}

func GetDeploymentAdmissionReceiptByDeployment(ctx context.Context, tx Tx, deploymentID uuid.UUID) (*DeploymentAdmissionReceipt, error) {
	return readDeploymentAdmissionReceipt(ctx, tx, "", uuid.Nil, deploymentID)
}

func readDeploymentAdmissionReceipt(ctx context.Context, tx Tx, orgID string, admissionID, deploymentID uuid.UUID) (*DeploymentAdmissionReceipt, error) {
	if tx == nil {
		return nil, errors.New("transaction is required")
	}
	query := `SELECT org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,carry_forward_module_versions,confirmed_restricted_version_uuids,
        core_epoch,disposition,deployment_id,COALESCE(terminal_outcome,''),core_settled_at,created_at
        FROM deployment_admission_receipts WHERE org_id=$1 AND admission_id=$2`
	args := []any{orgID, admissionID}
	if deploymentID != uuid.Nil {
		query = `SELECT org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,carry_forward_module_versions,confirmed_restricted_version_uuids,
            core_epoch,disposition,deployment_id,COALESCE(terminal_outcome,''),core_settled_at,created_at
            FROM deployment_admission_receipts WHERE deployment_id=$1`
		args = []any{deploymentID}
	}
	receipt := new(DeploymentAdmissionReceipt)
	var moduleVersions, carryForwardVersions, confirmedRestrictedVersions json.RawMessage
	if err := tx.QueryRowContext(ctx, query, args...).Scan(
		&receipt.OrgID, &receipt.AdmissionID, &receipt.ProjectUUID, &receipt.EnvironmentUUID, &receipt.ActorID,
		&receipt.RequestDigest, &receipt.Mode, &moduleVersions, &carryForwardVersions, &confirmedRestrictedVersions, &receipt.CoreEpoch, &receipt.Disposition,
		&receipt.DeploymentID, &receipt.TerminalOutcome, &receipt.CoreSettledAt, &receipt.CreatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, NewErrNotFound("Deployment admission receipt not found")
		}
		return nil, errors.Wrap(err, "failed to read Deployment admission receipt")
	}
	if err := json.Unmarshal(moduleVersions, &receipt.ModuleVersions); err != nil {
		return nil, errors.Wrap(err, "failed to decode Deployment admission Module Versions")
	}
	if err := json.Unmarshal(carryForwardVersions, &receipt.CarryForwardModuleVersions); err != nil {
		return nil, errors.Wrap(err, "failed to decode Deployment admission carry-forward Versions")
	}
	if err := json.Unmarshal(confirmedRestrictedVersions, &receipt.ConfirmedRestrictedVersionUUIDs); err != nil {
		return nil, errors.Wrap(err, "failed to decode Deployment admission restricted Version confirmations")
	}
	return receipt, nil
}

func PrepareDeploymentAdmissionReceipt(ctx context.Context, tx Tx, receipt DeploymentAdmissionReceipt) (*DeploymentAdmissionReceipt, error) {
	if tx == nil || receipt.OrgID == "" || receipt.AdmissionID == uuid.Nil || receipt.ProjectUUID == uuid.Nil ||
		receipt.EnvironmentUUID == uuid.Nil || receipt.ActorID == uuid.Nil || receipt.RequestDigest == "" ||
		receipt.CoreEpoch != 0 || receipt.Disposition != "pending" || receipt.DeploymentID.Valid || receipt.TerminalOutcome != "" {
		return nil, NewErrBadRequest("complete Deployment admission receipt identity is required")
	}
	if err := lockDeploymentAdmission(ctx, tx, receipt.OrgID, receipt.AdmissionID); err != nil {
		return nil, err
	}
	if existing, err := readDeploymentAdmissionReceipt(ctx, tx, receipt.OrgID, receipt.AdmissionID, uuid.Nil); err == nil {
		if !reflect.DeepEqual(receiptIntentIdentity(existing), receiptIntentIdentity(&receipt)) {
			return nil, NewErrConflict("Deployment admission identity already has a different durable outcome")
		}
		return existing, nil
	} else if _, missing := IsErrNotFound(err); !missing {
		return nil, err
	}
	moduleVersions, err := json.Marshal(receipt.ModuleVersions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode Deployment admission Module Versions")
	}
	carryForwardVersions, err := json.Marshal(receipt.CarryForwardModuleVersions)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode Deployment admission carry-forward Versions")
	}
	confirmedRestrictedVersions, err := json.Marshal(receipt.ConfirmedRestrictedVersionUUIDs)
	if err != nil {
		return nil, errors.Wrap(err, "failed to encode Deployment admission restricted Version confirmations")
	}
	if err := tx.QueryRowContext(ctx, `INSERT INTO deployment_admission_receipts
        (org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,carry_forward_module_versions,confirmed_restricted_version_uuids,core_epoch,disposition,deployment_id,terminal_outcome)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) RETURNING created_at`,
		receipt.OrgID, receipt.AdmissionID, receipt.ProjectUUID, receipt.EnvironmentUUID, receipt.ActorID,
		receipt.RequestDigest, receipt.Mode, moduleVersions, carryForwardVersions, confirmedRestrictedVersions,
		receipt.CoreEpoch, receipt.Disposition, receipt.DeploymentID, nullableString(receipt.TerminalOutcome),
	).Scan(&receipt.CreatedAt); err != nil {
		return nil, errors.Wrap(err, "failed to create Deployment admission receipt")
	}
	return &receipt, nil
}

func receiptIntentIdentity(receipt *DeploymentAdmissionReceipt) any {
	return struct {
		OrgID                                              string
		AdmissionID, ProjectUUID, EnvironmentUUID, ActorID uuid.UUID
		RequestDigest                                      string
		Mode                                               DeploymentMode
		ModuleVersions                                     map[string]uuid.UUID
		CarryForwardModuleVersions                         map[string]uuid.UUID
		ConfirmedRestrictedVersionUUIDs                    []uuid.UUID
	}{receipt.OrgID, receipt.AdmissionID, receipt.ProjectUUID, receipt.EnvironmentUUID, receipt.ActorID, receipt.RequestDigest,
		receipt.Mode, receipt.ModuleVersions, receipt.CarryForwardModuleVersions, receipt.ConfirmedRestrictedVersionUUIDs}
}

func BindDeploymentAdmissionReceipt(ctx context.Context, tx Tx, intent DeploymentAdmissionReceipt, deploymentID uuid.UUID, coreEpoch int64) (*DeploymentAdmissionReceipt, error) {
	if tx == nil || deploymentID == uuid.Nil || coreEpoch <= 0 {
		return nil, NewErrBadRequest("authorized Deployment admission and Deployment ID are required")
	}
	existing, err := GetDeploymentAdmissionReceipt(ctx, tx, intent.OrgID, intent.AdmissionID)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(receiptIntentIdentity(existing), receiptIntentIdentity(&intent)) {
		return nil, NewErrConflict("Deployment admission intent changed before binding")
	}
	if existing.Disposition == "committed" && existing.DeploymentID.Valid && existing.DeploymentID.UUID == deploymentID && existing.CoreEpoch == coreEpoch {
		return existing, nil
	}
	if existing.Disposition != "pending" {
		return nil, NewErrConflict("Deployment admission already has a different durable outcome")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE deployment_admission_receipts
        SET disposition='committed',deployment_id=$3,core_epoch=$4
        WHERE org_id=$1 AND admission_id=$2 AND disposition='pending'`, intent.OrgID, intent.AdmissionID, deploymentID, coreEpoch); err != nil {
		return nil, errors.Wrap(err, "failed to bind Deployment admission receipt")
	}
	return readDeploymentAdmissionReceipt(ctx, tx, intent.OrgID, intent.AdmissionID, uuid.Nil)
}

func SealDeploymentAdmissionAbsent(ctx context.Context, tx Tx, intent DeploymentAdmissionReceipt, coreEpoch int64) (*DeploymentAdmissionReceipt, error) {
	if tx == nil || coreEpoch < 0 {
		return nil, NewErrBadRequest("Deployment admission transaction and valid Core epoch are required")
	}
	existing, err := GetDeploymentAdmissionReceipt(ctx, tx, intent.OrgID, intent.AdmissionID)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(receiptIntentIdentity(existing), receiptIntentIdentity(&intent)) {
		return nil, NewErrConflict("Deployment admission intent changed before sealing absence")
	}
	if existing.Disposition == "committed" {
		return nil, NewErrConflict("committed Deployment admission cannot be sealed absent")
	}
	if existing.Disposition == "absent" {
		return existing, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE deployment_admission_receipts
        SET disposition='absent',terminal_outcome='absent',core_epoch=$3
        WHERE org_id=$1 AND admission_id=$2 AND disposition='pending'`, intent.OrgID, intent.AdmissionID, coreEpoch); err != nil {
		return nil, errors.Wrap(err, "failed to seal absent Deployment admission")
	}
	return readDeploymentAdmissionReceipt(ctx, tx, intent.OrgID, intent.AdmissionID, uuid.Nil)
}

func ListUnsettledDeploymentAdmissionReceipts(ctx context.Context, tx Tx, limit int, createdBefore time.Time) ([]DeploymentAdmissionReceipt, error) {
	if tx == nil || limit < 1 || limit > 1000 || createdBefore.IsZero() {
		return nil, NewErrBadRequest("transaction, bounded receipt limit and recovery cutoff are required")
	}
	rows, err := tx.QueryContext(ctx, `SELECT org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,carry_forward_module_versions,confirmed_restricted_version_uuids,
        core_epoch,disposition,deployment_id,COALESCE(terminal_outcome,''),core_settled_at,created_at
        FROM deployment_admission_receipts
        WHERE core_settled_at IS NULL AND created_at <= $2
        ORDER BY created_at,org_id,admission_id LIMIT $1`, limit, createdBefore)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list unsettled Deployment admissions")
	}
	defer func() { _ = rows.Close() }()
	receipts := make([]DeploymentAdmissionReceipt, 0)
	for rows.Next() {
		var receipt DeploymentAdmissionReceipt
		var moduleVersions, carryForwardVersions, confirmedRestrictedVersions json.RawMessage
		if err := rows.Scan(&receipt.OrgID, &receipt.AdmissionID, &receipt.ProjectUUID, &receipt.EnvironmentUUID, &receipt.ActorID,
			&receipt.RequestDigest, &receipt.Mode, &moduleVersions, &carryForwardVersions, &confirmedRestrictedVersions, &receipt.CoreEpoch, &receipt.Disposition,
			&receipt.DeploymentID, &receipt.TerminalOutcome, &receipt.CoreSettledAt, &receipt.CreatedAt); err != nil {
			return nil, errors.Wrap(err, "failed to scan unsettled Deployment admission")
		}
		if err := json.Unmarshal(moduleVersions, &receipt.ModuleVersions); err != nil {
			return nil, errors.Wrap(err, "failed to decode unsettled Deployment admission Module Versions")
		}
		if err := json.Unmarshal(carryForwardVersions, &receipt.CarryForwardModuleVersions); err != nil {
			return nil, errors.Wrap(err, "failed to decode unsettled Deployment admission carry-forward Versions")
		}
		if err := json.Unmarshal(confirmedRestrictedVersions, &receipt.ConfirmedRestrictedVersionUUIDs); err != nil {
			return nil, errors.Wrap(err, "failed to decode unsettled Deployment admission restricted Version confirmations")
		}
		receipts = append(receipts, receipt)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to iterate unsettled Deployment admissions")
	}
	return receipts, nil
}

func MarkDeploymentAdmissionCoreSettled(ctx context.Context, tx Tx, orgID string, admissionID uuid.UUID, outcome string, coreEpoch int64) error {
	if tx == nil {
		return errors.New("transaction is required")
	}
	if coreEpoch <= 0 {
		return NewErrBadRequest("positive Core admission epoch is required")
	}
	if err := lockDeploymentAdmission(ctx, tx, orgID, admissionID); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE deployment_admission_receipts SET terminal_outcome=$3,core_epoch=$4,core_settled_at=now()
        WHERE org_id=$1 AND admission_id=$2 AND (terminal_outcome IS NULL OR terminal_outcome=$3)
        AND (core_epoch=0 OR core_epoch=$4) AND ((disposition='absent' AND $3='absent') OR (disposition='committed' AND $3<>'absent'))`, orgID, admissionID, outcome, coreEpoch)
	if err != nil {
		return errors.Wrap(err, "failed to mark Deployment admission settled")
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return NewErrConflict("Deployment admission settlement identity changed")
	}
	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
