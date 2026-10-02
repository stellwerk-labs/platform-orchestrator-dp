package api

import (
	"context"
	"database/sql"

	"github.com/google/uuid"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func externalRollbackScope(orgID, pluginID string, body ExternalDeploymentAdmissionBody) model.ExternalOperationRollbackAnchor {
	return model.ExternalOperationRollbackAnchor{OrgID: orgID, PluginID: pluginID, OperationRef: body.OperationRef.String(),
		ProjectID: body.ProjectId, EnvID: body.EnvId, ProjectUUID: body.ProjectUuid, EnvironmentUUID: body.EnvironmentUuid}
}

func (a *ExternalDeploymentHTTPAdapter) operationRollbackAnchor(ctx context.Context, tx model.Tx, scope model.ExternalOperationRollbackAnchor, lock bool) (*model.ExternalOperationRollbackAnchor, error) {
	read := model.ReadExternalOperationRollbackAnchor
	if lock {
		read = model.LockExternalOperationRollbackAnchor
	}
	anchor, err := read(ctx, tx, scope)
	if err != nil {
		return nil, err
	}
	if anchor == nil {
		// Only a single fully verifiable legacy forward can be adopted. Neither
		// timestamps nor equivalent versions establish a historical first base.
		var count int
		var first sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT count(*), min(deployment_id::text)
			FROM external_operation_deployments WHERE org_id=$1 AND plugin_id=$2 AND operation_ref=$3
			AND (environment_uuid=$4 OR (project_id=$5 AND env_id=$6)) AND service_action=$7`,
			scope.OrgID, scope.PluginID, scope.OperationRef, scope.EnvironmentUUID, scope.ProjectID, scope.EnvID, ExternalDeploymentAction).Scan(&count, &first); err != nil {
			return nil, err
		}
		if count == 0 {
			return nil, nil
		}
		if count != 1 || !first.Valid {
			return nil, model.NewErrConflict("legacy operation has ambiguous first forward lineage")
		}
		firstID, err := uuid.Parse(first.String)
		if err != nil {
			return nil, err
		}
		receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, scope.OrgID, scope.PluginID, firstID)
		if err != nil || !externalReceiptOwnsScope(receipt, scope) || !receipt.BaseDeploymentID.IsSet() {
			return nil, model.NewErrConflict("legacy first forward lineage cannot be verified")
		}
		value := scope
		value.FirstForwardDeploymentID, value.RollbackBaseDeploymentID = firstID, receipt.BaseDeploymentID.Must()
		anchor = &value
	}
	if err := a.validateRollbackAnchor(ctx, tx, *anchor); err != nil {
		return nil, err
	}
	if lock {
		stored, err := model.ReadExternalOperationRollbackAnchor(ctx, tx, scope)
		if err != nil {
			return nil, err
		}
		if stored == nil {
			if err := model.CreateExternalOperationRollbackAnchor(ctx, tx, *anchor); err != nil {
				return nil, err
			}
		}
	}
	return anchor, nil
}

func externalReceiptOwnsScope(receipt *model.ExternalOperationDeploymentReceipt, scope model.ExternalOperationRollbackAnchor) bool {
	return receipt != nil && receipt.OperationRef == scope.OperationRef && receipt.ProjectID == scope.ProjectID && receipt.EnvID == scope.EnvID &&
		receipt.ProjectUUID.IsSet() && receipt.ProjectUUID.Must() == scope.ProjectUUID && receipt.EnvironmentUUID.IsSet() && receipt.EnvironmentUUID.Must() == scope.EnvironmentUUID
}

func (a *ExternalDeploymentHTTPAdapter) validateRollbackAnchor(ctx context.Context, tx model.Tx, anchor model.ExternalOperationRollbackAnchor) error {
	first, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, anchor.OrgID, anchor.PluginID, anchor.FirstForwardDeploymentID)
	if err != nil || !externalReceiptOwnsScope(first, anchor) || first.ServiceAction != ExternalDeploymentAction ||
		!first.BaseDeploymentID.IsSet() || first.BaseDeploymentID.Must() != anchor.RollbackBaseDeploymentID {
		return model.NewErrConflict("first forward receipt does not verify the immutable rollback anchor")
	}
	forward, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, anchor.OrgID, anchor.FirstForwardDeploymentID, model.GetModeDefault)
	if err != nil || forward.Mode != model.DeploymentModeDeploy || forward.DeploymentEnvUuid != anchor.EnvironmentUUID || forward.ProjectId != anchor.ProjectID || forward.EnvId != anchor.EnvID {
		return model.NewErrConflict("first forward Deployment is not retained in this Environment")
	}
	return a.validateRollbackBase(ctx, tx, anchor)
}

func (a *ExternalDeploymentHTTPAdapter) validateRollbackBase(ctx context.Context, tx model.Tx, anchor model.ExternalOperationRollbackAnchor) error {
	base, manifest, tofu, graph, err := a.Admission.Database.GetDeployment(ctx, tx, anchor.OrgID, anchor.RollbackBaseDeploymentID, model.GetModeDefault)
	if err != nil || base.Status != model.DeploymentStatusSucceeded || !base.CompletedAt.IsSet() || base.DeploymentEnvUuid != anchor.EnvironmentUUID ||
		base.ProjectId != anchor.ProjectID || base.EnvId != anchor.EnvID || !model.CompleteDeploymentSnapshot(manifest, tofu, graph) {
		return model.NewErrConflict("immutable rollback base is not a complete retained successful Deployment")
	}
	return nil
}
