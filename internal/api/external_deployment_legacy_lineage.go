package api

import (
	"context"
	"errors"

	"github.com/google/uuid"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

// Durable predecessor provenance is distinct from terminal execution proof.
// Legacy completion projection must never create authority for a new retry.
func (a *ExternalDeploymentHTTPAdapter) rollbackRetryProvenanceRecorded(ctx context.Context, tx model.Tx, deployment model.DeploymentSummary, receipt model.ExternalOperationDeploymentReceipt, anchor model.ExternalOperationRollbackAnchor) (bool, error) {
	if receipt.ServiceAction != ExternalDeploymentRollbackAction || !externalReceiptOwnsScope(&receipt, anchor) ||
		!receipt.BaseDeploymentID.IsSet() || receipt.BaseDeploymentID.Must() != anchor.RollbackBaseDeploymentID ||
		!deployment.RollbackToId.IsSet() || deployment.RollbackToId.Must() != anchor.RollbackBaseDeploymentID || !receipt.ForwardDeploymentID.IsSet() {
		return false, nil
	}
	forwardID := receipt.ForwardDeploymentID.Must()
	forwardReceipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, anchor.OrgID, anchor.PluginID, forwardID)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return false, nil
		}
		return false, err
	}
	if !externalReceiptOwnsScope(forwardReceipt, anchor) || forwardReceipt.ServiceAction != ExternalDeploymentAction {
		return false, nil
	}
	forward, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, anchor.OrgID, forwardID, model.GetModeDefault)
	if err != nil {
		return false, err
	}
	if forward.Mode != model.DeploymentModeDeploy || forward.DeploymentEnvUuid != anchor.EnvironmentUUID || forward.ProjectId != anchor.ProjectID || forward.EnvId != anchor.EnvID {
		return false, nil
	}
	if !receipt.RetryOfRollbackDeploymentID.IsSet() {
		return true, nil
	}
	previousID := receipt.RetryOfRollbackDeploymentID.Must()
	previousReceipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, anchor.OrgID, anchor.PluginID, previousID)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return false, nil
		}
		return false, err
	}
	if !externalReceiptOwnsScope(previousReceipt, anchor) || previousReceipt.ServiceAction != ExternalDeploymentRollbackAction ||
		!previousReceipt.BaseDeploymentID.IsSet() || previousReceipt.BaseDeploymentID.Must() != anchor.RollbackBaseDeploymentID ||
		!previousReceipt.ForwardDeploymentID.IsSet() || previousReceipt.ForwardDeploymentID.Must() != forwardID || previousID == deployment.Id {
		return false, nil
	}
	previous, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, anchor.OrgID, previousID, model.GetModeDefault)
	if err != nil {
		return false, err
	}
	if previous.Mode != model.DeploymentModeRollback || previous.DeploymentEnvUuid != anchor.EnvironmentUUID || previous.ProjectId != anchor.ProjectID || previous.EnvId != anchor.EnvID ||
		!previous.RollbackToId.IsSet() || previous.RollbackToId.Must() != anchor.RollbackBaseDeploymentID {
		return false, errors.New("retained rollback predecessor does not verify the receipt provenance")
	}
	return true, nil
}

// This projection supports completion observation of a provable first legacy
// rollback. It does not populate durable predecessor provenance or authorize a
// new retry; locked admission still requires the actual retained receipt field.
func (a *ExternalDeploymentHTTPAdapter) projectLegacyRollbackForward(ctx context.Context, tx model.Tx, deployment model.DeploymentSummary, receipt model.ExternalOperationDeploymentReceipt, anchor model.ExternalOperationRollbackAnchor) (*uuid.UUID, error) {
	if receipt.ServiceAction != ExternalDeploymentRollbackAction || !externalReceiptOwnsScope(&receipt, anchor) ||
		!deployment.RollbackToId.IsSet() || deployment.RollbackToId.Must() != anchor.RollbackBaseDeploymentID || receipt.RetryOfRollbackDeploymentID.IsSet() {
		return nil, nil
	}
	var forwards, rollbacks int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE service_action=$7), count(*) FILTER (WHERE service_action=$8)
		FROM external_operation_deployments WHERE org_id=$1 AND plugin_id=$2 AND operation_ref=$3
		AND (environment_uuid=$4 OR (project_id=$5 AND env_id=$6))`, anchor.OrgID, anchor.PluginID, anchor.OperationRef,
		anchor.EnvironmentUUID, anchor.ProjectID, anchor.EnvID, ExternalDeploymentAction, ExternalDeploymentRollbackAction).Scan(&forwards, &rollbacks); err != nil {
		return nil, err
	}
	if forwards != 1 || rollbacks != 1 {
		return nil, nil
	}
	forward, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, anchor.OrgID, anchor.FirstForwardDeploymentID, model.GetModeDefault)
	if err != nil || !forward.CompletedAt.IsSet() || (forward.Status != model.DeploymentStatusSucceeded && forward.Status != model.DeploymentStatusFailed) {
		return nil, nil
	}
	if forward.Status == model.DeploymentStatusFailed {
		var stopped bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runner_event_inbox
			WHERE org_id=$1 AND runner_id=$2 AND deployment_id=$3 AND event_type='deployment-result')`, anchor.OrgID, forward.RunnerId, forward.Id).Scan(&stopped); err != nil {
			return nil, err
		}
		if !stopped {
			return nil, nil
		}
	}
	return &anchor.FirstForwardDeploymentID, nil
}
