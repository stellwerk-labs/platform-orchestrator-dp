package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const (
	pipelinePlanReady       = "ready"
	pipelinePlanPending     = "pending"
	pipelinePlanBusy        = "busy"
	pipelinePlanUsed        = "used"
	pipelinePlanSuperseded  = "superseded"
	pipelinePlanUnavailable = "unavailable"
)

// Capture is private admission evidence; only PipelinePlanStatus is public.
type pipelinePlanCapture struct {
	NativeTarget     *cp.NativeExecutionTargetReference `json:"native_target,omitempty"`
	BaseDeploymentID uuid.UUID                          `json:"base_deployment_id"`
	Request          deploymentRequest                  `json:"request"`
	Lifecycle        retainedLifecycle                  `json:"lifecycle"`
	MaterialSHA256   string                             `json:"material_sha256"`
	StorageSHA256    string                             `json:"storage_sha256"`
}

type pipelinePlanStatus struct {
	State               string     `json:"state"`
	Subject             string     `json:"subject,omitempty"`
	BaseDeploymentID    *uuid.UUID `json:"base_deployment_id,omitempty"`
	CurrentDeploymentID *uuid.UUID `json:"current_deployment_id,omitempty"`
	ApplyDeploymentID   *uuid.UUID `json:"apply_deployment_id,omitempty"`
	Reason              string     `json:"reason,omitempty"`
	CanReplan           bool       `json:"can_replan"`
}

func pipelineMaterialDigest(manifest model.EncodedDeploymentManifest, graph model.EncodedDeploymentGraph, tofu model.RawTofu, artifacts []model.ModuleArtifactRequirement, backend string) (string, error) {
	raw, err := json.Marshal(struct {
		Manifest, Graph, Tofu []byte
		Artifacts             []model.ModuleArtifactRequirement
		Backend               string
	}{manifest, graph, tofu, artifacts, backend})
	return hashRetained(raw), err
}

func readPipelinePlanCapture(ctx context.Context, tx model.Tx, id uuid.UUID, lock bool) (*pipelinePlanCapture, string, error) {
	query := `SELECT plan_capture,backend FROM deployment_pipeline_runs WHERE deployment_id=$1`
	if lock {
		query += ` FOR UPDATE`
	}
	var raw []byte
	var backend string
	if err := tx.QueryRowContext(ctx, query, id).Scan(&raw, &backend); err != nil {
		return nil, "", err
	}
	if len(raw) == 0 {
		return nil, backend, nil
	}
	var capture pipelinePlanCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		return nil, "", err
	}
	return &capture, backend, nil
}

func (s *Server) pipelinePlanStatus(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, run *model.DeploymentPipeline) (*pipelinePlanStatus, error) {
	if !model.IsPlanDeployment(dep.Mode) {
		return nil, nil
	}
	capture, _, err := readPipelinePlanCapture(ctx, tx, dep.Id, false)
	if err != nil {
		return nil, err
	}
	status := &pipelinePlanStatus{State: pipelinePlanUnavailable, Reason: "This plan has no verifiable saved-plan admission snapshot. Run a new manual Plan."}
	if dep.Mode != model.DeploymentModeDeployPlan || capture == nil {
		return status, nil
	}
	status.Subject = run.Progress.Subject
	status.BaseDeploymentID = &capture.BaseDeploymentID
	var head uuid.NullUUID
	var child uuid.NullUUID
	var busy bool
	err = tx.QueryRowContext(ctx, `SELECT effective.deployment_id,c.deployment_id,
 EXISTS(SELECT 1 FROM deployments running WHERE running.id=e.last_executing_deployment_id AND running.status='executing')
 OR EXISTS(SELECT 1 FROM deployment_pipeline_runs p JOIN deployments d ON d.id=p.deployment_id WHERE d.de_id=e.id AND NOT p.settled AND p.definition->>'strategy'<>'plan')
	 FROM deployment_environments e JOIN deployment_effective_state effective ON effective.environment_id=e.id LEFT JOIN deployment_pipeline_runs c ON c.source_plan_id=$1 WHERE e.id=$2`, dep.Id, dep.DeploymentEnvUuid).Scan(&head, &child, &busy)
	if err != nil {
		return nil, err
	}
	status.CurrentDeploymentID = &head.UUID
	if child.Valid {
		status.State = pipelinePlanUsed
		status.ApplyDeploymentID = &child.UUID
		status.Reason = "This saved plan is already bound to an Apply deployment."
		return status, nil
	}
	return completePipelinePlanStatus(status, dep, run, head.UUID, busy), nil
}

func completePipelinePlanStatus(status *pipelinePlanStatus, dep *model.DeploymentSummary, run *model.DeploymentPipeline, head uuid.UUID, busy bool) *pipelinePlanStatus {
	if dep.Status == model.DeploymentStatusExecuting {
		status.State = pipelinePlanPending
		status.Reason = "Wait for this Plan to finish."
		return status
	}
	status.CanReplan = !busy
	if dep.Status != model.DeploymentStatusSucceeded || run.Progress.Subject == "" {
		status.Reason = "This Plan has no successful saved artifact. Create a new plan."
		return status
	}
	expected := *status.BaseDeploymentID
	if expected == uuid.Nil && head == dep.Id {
		expected = dep.Id
	}
	if head != expected {
		status.State = pipelinePlanSuperseded
		status.Reason = "A newer deployment changed this plan's environment base."
		if busy {
			status.Reason = "A newer deployment is still running. Create a new plan after it finishes."
		}
		return status
	}
	if busy {
		status.State = pipelinePlanBusy
		status.Reason = "Another deployment is still executing in this environment."
		return status
	}
	status.State = pipelinePlanReady
	status.Reason = ""
	return status
}

func readPipelineApply(ctx context.Context, tx model.Tx, source uuid.UUID) (uuid.NullUUID, string, error) {
	var child uuid.UUID
	var subject string
	err := tx.QueryRowContext(ctx, `SELECT deployment_id,source_plan_subject FROM deployment_pipeline_runs WHERE source_plan_id=$1`, source).Scan(&child, &subject)
	if errors.Is(err, sql.ErrNoRows) {
		return uuid.NullUUID{}, "", nil
	}
	return uuid.NullUUID{UUID: child, Valid: err == nil}, subject, err
}

func commandPlanAuthority(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary) (*pipelinePlanBase, *pipelineSourcePlan, error) {
	if dep.Mode == model.DeploymentModeObservation {
		return nil, nil, nil
	}

	var capture, sourceCapture []byte
	var source uuid.NullUUID
	var subject sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT c.plan_capture,c.source_plan_id,c.source_plan_subject,s.plan_capture FROM deployment_pipeline_runs c LEFT JOIN deployment_pipeline_runs s ON s.deployment_id=c.source_plan_id WHERE c.deployment_id=$1`, dep.Id).
		Scan(&capture, &source, &subject, &sourceCapture)
	if err != nil {
		return nil, nil, err
	}
	var base *pipelinePlanBase
	if len(capture) > 0 {
		var saved pipelinePlanCapture
		if err = json.Unmarshal(capture, &saved); err != nil {
			return nil, nil, err
		}
		if !sameCommandTarget(saved.NativeTarget, dep.NativeTarget) {
			return nil, nil, errors.New("plan command target changed")
		}
		base = &pipelinePlanBase{BaseDeploymentID: saved.BaseDeploymentID.String()}
	}
	if !source.Valid {
		return base, nil, nil
	}
	var saved pipelinePlanCapture
	if len(sourceCapture) == 0 || json.Unmarshal(sourceCapture, &saved) != nil || !subject.Valid || subject.String == "" {
		return nil, nil, errors.New("saved-plan dispatch authority unavailable")
	}
	if !sameCommandTarget(saved.NativeTarget, dep.NativeTarget) {
		return nil, nil, errors.New("source plan command target changed")
	}
	return base, &pipelineSourcePlan{SourcePlanID: source.UUID.String(), Subject: subject.String, BaseDeploymentID: saved.BaseDeploymentID.String()}, nil
}

func storePipelinePlanCapture(ctx context.Context, tx model.Tx, id uuid.UUID, capture pipelinePlanCapture) error {
	encoded, err := json.Marshal(capture)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET plan_capture=$2 WHERE deployment_id=$1 AND plan_capture IS NULL`, id, encoded)
	return err
}
