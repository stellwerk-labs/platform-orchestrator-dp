package model

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
)

func validateAdmissionNativeIntent(receipt *DeploymentAdmissionReceipt) error {
	native := receipt.NativeTarget != nil && validNativeExecutionTarget(receipt.NativeTarget, receipt.NativeTarget.RunnerId)
	if (receipt.NativeTarget != nil && !native) ||
		((receipt.Mode == DeploymentModeDeployPlan || receipt.Mode == DeploymentModeRollbackPlan) && !native) {
		return NewErrConflict("Deployment admission requires a valid Native execution target")
	}
	if watermark := receipt.NativeAdmissionWatermark; watermark != nil && (!native || watermark.AdmissionId == uuid.Nil || watermark.Epoch <= 0) {
		return NewErrConflict("Deployment admission predecessor requires a valid Native target, admission ID and positive epoch")
	}
	return nil
}

// LatestNativeAdmissionWatermark reads retained admission knowledge, independently
// of effective state and Deployment retention. Core still proves global bootstrap
// when no local receipt exists.
func LatestNativeAdmissionWatermark(ctx context.Context, tx Tx, org string, project, environment uuid.UUID) (*cp.NativeAdmissionWatermark, error) {
	if tx == nil || org == "" || project == uuid.Nil || environment == uuid.Nil {
		return nil, NewErrBadRequest("transaction and exact Native admission scope are required")
	}
	var unresolved bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM deployment_admission_receipts
 WHERE org_id=$1 AND project_uuid=$2 AND environment_uuid=$3
 AND (core_settled_at IS NULL OR core_epoch<=0 OR
      (disposition='absent' AND (terminal_outcome IS DISTINCT FROM 'absent' OR deployment_id IS NOT NULL)) OR
      (disposition='committed' AND (deployment_id IS NULL OR terminal_outcome IS NULL OR terminal_outcome NOT IN ('succeeded','failed','cancelled','terminated'))) OR
      disposition NOT IN ('absent','committed')))`, org, project, environment).Scan(&unresolved)
	if err != nil {
		return nil, err
	}
	if unresolved {
		return nil, NewErrConflict("Native admission history has unresolved outcome evidence")
	}
	rows, err := tx.QueryContext(ctx, `SELECT admission_id,core_epoch,actor_id,request_digest,mode,native_target,native_admission_watermark
 FROM deployment_admission_receipts
 WHERE org_id=$1 AND project_uuid=$2 AND environment_uuid=$3 AND disposition='committed'
 ORDER BY core_epoch DESC LIMIT 2`, org, project, environment)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var latest *cp.NativeAdmissionWatermark
	for rows.Next() {
		var receipt DeploymentAdmissionReceipt
		var target, predecessor []byte
		if err := rows.Scan(&receipt.AdmissionID, &receipt.CoreEpoch, &receipt.ActorID, &receipt.RequestDigest, &receipt.Mode, &target, &predecessor); err != nil {
			return nil, err
		}
		if latest != nil {
			if receipt.CoreEpoch == latest.Epoch {
				return nil, NewErrConflict("Native admission history has an ambiguous latest epoch")
			}
			continue
		}
		if err := decodeNativeAdmissionKnowledge(&receipt, target, predecessor); err != nil {
			return nil, err
		}
		latest = &cp.NativeAdmissionWatermark{AdmissionId: receipt.AdmissionID, Epoch: receipt.CoreEpoch}
	}
	return latest, rows.Err()
}

// Stored target and predecessor evidence must satisfy the same closed object
// contract as a new intent; unknown fields cannot silently qualify old history.
func decodeNativeAdmissionKnowledge(receipt *DeploymentAdmissionReceipt, target, predecessor []byte) error {
	var targetFields map[string]json.RawMessage
	if err := json.Unmarshal(target, &targetFields); err != nil {
		return NewErrConflict("Native admission history has malformed target evidence")
	}
	// JSON DTO decoding accepts case-insensitive aliases; stored keys must be exact.
	for field := range targetFields {
		switch field {
		case "runner_id", "runner_uuid", "runtime_version", "pipeline_version", "tools_digest", "state_backend_type", "state_configuration_digest":
		default:
			return NewErrConflict("Native admission history has unknown target fields")
		}
	}
	if err := json.Unmarshal(target, &receipt.NativeTarget); err != nil {
		return NewErrConflict("Native admission history has malformed target evidence")
	}
	if len(predecessor) > 0 {
		var predecessorFields map[string]json.RawMessage
		if err := json.Unmarshal(predecessor, &predecessorFields); err != nil {
			return NewErrConflict("Native admission history has malformed predecessor evidence")
		}
		for field := range predecessorFields {
			switch field {
			case "admission_id", "epoch":
			default:
				return NewErrConflict("Native admission history has unknown predecessor fields")
			}
		}
		if err := json.Unmarshal(predecessor, &receipt.NativeAdmissionWatermark); err != nil {
			return NewErrConflict("Native admission history has malformed predecessor evidence")
		}
	}
	if receipt.AdmissionID == uuid.Nil || receipt.ActorID == uuid.Nil || !nativeCommandDigest(receipt.RequestDigest) || receipt.CoreEpoch <= 0 || receipt.NativeTarget == nil {
		return NewErrConflict("Native admission history has malformed or legacy evidence")
	}
	switch receipt.Mode {
	case DeploymentModeDeploy, DeploymentModeRollback, DeploymentModeDestroy, DeploymentModeDeployPlan, DeploymentModeRollbackPlan:
		return validateAdmissionNativeIntent(receipt)
	default:
		return NewErrConflict("Native admission history has an invalid mode")
	}
}
