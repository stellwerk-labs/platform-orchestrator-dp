package api

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
)

const pipelineCompleted = "completed"

func pluginPipelineLifecycle(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary) (*DeploymentPipelineLifecycle, bool, string, error) {
	run, err := model.ReadDeploymentPipeline(ctx, tx, dep.Id)
	if err != nil || run == nil {
		return nil, false, string(dep.Status), err
	}
	out, err := pipelineLifecycleIdentity(run.Definition)
	if err != nil {
		return nil, false, "", err
	}
	out.Phase = run.Progress.Phase
	out.InfrastructureApplied = run.Progress.InfrastructureApplied
	if run.Progress.Subject != "" {
		out.PlanSubject = &run.Progress.Subject
	}
	out.AttentionReason = pipelineAttentionReason(run.Progress.Phase)
	closed, err := pipelineAdmissionClosed(ctx, tx, dep.Id)
	if err != nil {
		return nil, false, "", err
	}
	var settled bool
	if err = tx.QueryRowContext(ctx, `SELECT settled FROM deployment_pipeline_runs WHERE deployment_id=$1`, dep.Id).Scan(&settled); err != nil {
		return nil, false, "", err
	}
	status := string(dep.Status)
	if !settled || !closed {
		status = string(model.DeploymentStatusExecuting)
	} else if run.Progress.Phase != pipelineCompleted {
		status = string(model.DeploymentStatusFailed)
	}
	return out, closed, status, nil
}
func (a *ExternalDeploymentHTTPAdapter) observationRedeployReceipt(ctx context.Context, tx model.Tx, org string, requestID, deploymentID, baselineID uuid.UUID) (observation.ObservationRedeployReceipt, error) {
	dep, manifest, tofu, graph, err := a.Admission.Database.GetDeployment(ctx, tx, org, deploymentID, model.GetModeDefault)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return observation.ObservationRedeployReceipt{RequestId: requestID, DeploymentId: deploymentID, BaselineId: baselineID, Status: observation.ObservationRedeployReceiptStatusUnavailable}, nil
		}
		return observation.ObservationRedeployReceipt{}, err
	}
	pipeline, closed, status, err := pluginPipelineLifecycle(ctx, tx, dep)
	if err != nil {
		return observation.ObservationRedeployReceipt{}, err
	}
	out := observation.ObservationRedeployReceipt{RequestId: requestID, DeploymentId: deploymentID, BaselineId: baselineID, Status: observation.ObservationRedeployReceiptStatus(status), AdmissionClosed: &closed}
	if pipeline != nil {
		raw, _ := json.Marshal(pipeline)
		if err = json.Unmarshal(raw, &out.Pipeline); err != nil {
			return out, err
		}
	}
	if status == string(model.DeploymentStatusSucceeded) {
		fingerprint := model.ObservationBaselineFingerprint(manifest, tofu, graph, dep.ModuleArtifacts, dep.NativeTarget)
		out.ResultingBaselineFingerprint = &fingerprint
	}
	return out, nil
}
func pipelineLifecycleIdentity(definition model.PipelineDefinition) (*DeploymentPipelineLifecycle, error) {
	reference := p.BuiltinReference()
	var pipelineID, versionID *uuid.UUID
	if definition.PipelineID != "" {
		versionUUID, err := uuid.Parse(definition.ID)
		if err != nil {
			return nil, err
		}
		pipelineUUID, err := uuid.Parse(definition.PipelineID)
		if err != nil {
			return nil, err
		}
		versionID, pipelineID = &versionUUID, &pipelineUUID
		reference = p.PipelineReference{VersionId: versionID, Digest: definition.Digest}
	}
	raw, _ := json.Marshal(reference)
	var ref PipelineReference
	if err := json.Unmarshal(raw, &ref); err != nil {
		return nil, err
	}
	return &DeploymentPipelineLifecycle{Reference: ref, PipelineId: pipelineID, VersionId: versionID, Version: definition.Version, Digest: reference.Digest}, nil
}

func pipelineAttentionReason(phase string) *string {
	switch phase {
	case "awaiting_approval", "post_failed", "outcome_unknown", "failed", "denied":
		return &phase
	}
	return nil
}
