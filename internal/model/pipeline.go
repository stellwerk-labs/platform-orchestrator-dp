package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
)

const (
	PipelineProtocol          = "stellwerk.pipeline/v1"
	PipelinePlanApplyProtocol = "stellwerk.pipeline.plan-apply/v1"
	PipelineDefault           = "default"
	PipelineReviewed          = "reviewed"
	PipelinePlan              = "plan"
	PipelineApplySaved        = "apply_saved"
)

type PipelineSelection struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
}
type PipelineStep struct {
	ID         string          `json:"id"`
	ToolID     string          `json:"tool_id"`
	Phase      string          `json:"phase"`
	Parameters json.RawMessage `json:"parameters"`
}
type PipelineApproval struct {
	Mode          string `json:"mode"`
	PolicyID      string `json:"policy_id,omitempty"`
	PolicyVersion string `json:"policy_version,omitempty"`
}
type PipelineCost struct {
	Enabled        bool  `json:"enabled"`
	PolicyRevision int64 `json:"policy_revision,omitempty"`
}
type PipelineDefinition struct {
	RequiredCapabilities   []string           `json:"required_capabilities,omitempty"`
	PipelineID             string             `json:"pipeline_id,omitempty"`
	ProjectID              string             `json:"project_id,omitempty"`
	Digest                 string             `json:"digest,omitempty"`
	IntendedStrategy       string             `json:"intended_strategy,omitempty"`
	Approval               *PipelineApproval  `json:"approval,omitempty"`
	Cost                   *PipelineCost      `json:"cost,omitempty"`
	RequestedSelection     *PipelineSelection `json:"requested_selection,omitempty"`
	RequestIdentityVersion int                `json:"request_identity_version,omitempty"`
	ID                     string             `json:"id"`
	Version                int                `json:"version"`
	Name                   string             `json:"name"`
	Strategy               string             `json:"strategy"`
	Steps                  []PipelineStep     `json:"steps"`
}
type PipelineProgress struct {
	PostRepair            *PipelinePostRepairAuthority `json:"post_repair,omitempty"`
	LogCapture            *nativelogs.Descriptor       `json:"log_capture,omitempty"`
	LogReferences         []nativelogs.Descriptor      `json:"log_references,omitempty"`
	LogReferencesLimited  bool                         `json:"log_references_limited,omitempty"`
	ID                    string                       `json:"id"`
	EnvironmentID         string                       `json:"environment_id"`
	Phase                 string                       `json:"phase"`
	Subject               string                       `json:"subject,omitempty"`
	ErrorCode             string                       `json:"error_code,omitempty"`
	FailedStep            string                       `json:"failed_step,omitempty"`
	Revision              int64                        `json:"revision"`
	CreatedAt             time.Time                    `json:"created_at"`
	LastReceivedAt        *time.Time                   `json:"last_received_at,omitempty"`
	InfrastructureApplied bool                         `json:"infrastructure_applied"`
	Changes               *struct {
		Add    int `json:"add"`
		Change int `json:"change"`
		Remove int `json:"remove"`
	} `json:"changes,omitempty"`
	PlanChanges      *PipelineChanges          `json:"plan_changes,omitempty"`
	ApplyChanges     *PipelineChanges          `json:"apply_changes,omitempty"`
	Cost             *PipelineCostComparison   `json:"cost,omitempty"`
	ApprovalDecision *PipelineApprovalDecision `json:"approval_decision,omitempty"`
	Steps            []PipelineStepResult      `json:"steps"`
}
type DeploymentPipeline struct {
	Definition      PipelineDefinition `json:"definition"`
	Progress        PipelineProgress   `json:"progress"`
	SourcePlanID    *uuid.UUID         `json:"source_plan_id,omitempty"`
	ReplannedFromID *uuid.UUID         `json:"replanned_from_id,omitempty"`
}

func PipelineDefinitions() []PipelineDefinition {
	pre := PipelineStep{ID: "validate-manifest", ToolID: "validate-manifest", Phase: "pre_plan", Parameters: json.RawMessage(`{}`)}
	check := PipelineStep{ID: "resource-policy", ToolID: "resource-policy", Phase: "plan_check", Parameters: json.RawMessage(`{}`)}
	post := PipelineStep{ID: "bootstrap-lost-ack", ToolID: "bootstrap-lost-ack", Phase: "post_apply", Parameters: json.RawMessage(`{}`)}
	return []PipelineDefinition{
		{ID: PipelineDefault, Version: 1, Name: "Apply", Strategy: "apply", Steps: []PipelineStep{}},
		{ID: PipelineReviewed, Version: 1, Name: "Plan and approval", Strategy: PipelineReviewed, Steps: []PipelineStep{pre, check}},
		{ID: "reviewed-bootstrap", Version: 1, Name: "Plan, approval and database bootstrap", Strategy: PipelineReviewed, Steps: []PipelineStep{pre, check, post}},
	}
}

func ResolvePipeline(selection *PipelineSelection, mode DeploymentMode) (*PipelineDefinition, error) {
	if selection == nil {
		selection = &PipelineSelection{ID: PipelineDefault, Version: 1}
	}
	if mode == DeploymentModeObservation {
		return nil, NewErrBadRequest("pipeline Runner does not support observations")
	}
	for _, definition := range PipelineDefinitions() {
		if definition.ID != selection.ID || definition.Version != selection.Version {
			continue
		}
		if IsPlanDeployment(mode) {
			if definition.ID != PipelineDefault {
				return nil, NewErrBadRequest("manual Plan cannot select an Apply pipeline")
			}
			definition.Name, definition.Strategy = "Plan", "plan"
		} else if mode == DeploymentModeDestroy {
			if definition.ID != PipelineDefault {
				return nil, NewErrBadRequest("Destroy supports only the default pipeline")
			}
			definition.Name, definition.Strategy = "Destroy", "destroy"
		}
		return &definition, nil
	}
	return nil, NewErrBadRequest("unknown pipeline version")
}

func InsertDeploymentPipeline(ctx context.Context, tx Tx, dep *DeploymentSummary, definition *PipelineDefinition, backend string, base uuid.UUID) error {
	if definition == nil {
		return nil
	}
	costPolicy, err := RequirePipelineCostPolicy(ctx, tx, definition)
	if err != nil {
		return err
	}
	if costPolicy != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO deployment_cost_policy_snapshots(deployment_id,enabled,policy_revision) VALUES($1,$2,$3)`, dep.Id, costPolicy.Enabled, costPolicy.PolicyRevision); err != nil {
			return err
		}
	}
	pinned, err := json.Marshal(definition)
	if err != nil {
		return err
	}
	progress, err := json.Marshal(PipelineProgress{ID: dep.Id.String(), EnvironmentID: dep.DeploymentEnvUuid.String(), Phase: "accepted", CreatedAt: dep.CreatedAt, Steps: []PipelineStepResult{}})
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO deployment_pipeline_runs(deployment_id,definition,progress,backend,effect_base_deployment_id,effect_base_captured)
 VALUES($1,$2,$3,$4,(SELECT id FROM deployments WHERE id=$5 AND mode NOT IN ('plan_only','rollback_plan','observation')),true)`, dep.Id, pinned, progress, backend, base)
	return err
}

func ReadDeploymentPipeline(ctx context.Context, tx Tx, id uuid.UUID) (*DeploymentPipeline, error) {
	var definition, progress []byte
	var source, parent uuid.NullUUID
	err := tx.QueryRowContext(ctx, `SELECT definition,progress,source_plan_id,replanned_from_id FROM deployment_pipeline_runs WHERE deployment_id=$1`, id).Scan(&definition, &progress, &source, &parent)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var p DeploymentPipeline
	if err = json.Unmarshal(definition, &p.Definition); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(progress, &p.Progress); err != nil {
		return nil, err
	}
	if source.Valid {
		p.SourcePlanID = &source.UUID
	}
	if parent.Valid {
		p.ReplannedFromID = &parent.UUID
	}
	return &p, nil
}

func RequirePipelineStopped(ctx context.Context, tx Tx, environment uuid.UUID) error {
	var blocked bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM deployment_pipeline_runs p JOIN deployments d ON d.id=p.deployment_id WHERE d.de_id=$1 AND NOT p.settled AND p.definition->>'strategy'<>'plan')`, environment).Scan(&blocked)
	if err != nil {
		return err
	}
	if blocked {
		return NewErrCodedConflict(CodeEnvironmentBusy, "Environment has an unsettled pipeline execution")
	}
	return nil
}

func SettleDeploymentPipeline(ctx context.Context, tx Tx, id uuid.UUID, status DeploymentStatus) error {
	phase := "failed"
	if status == DeploymentStatusSucceeded {
		phase = "completed"
	}
	_, err := tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET settled=true,progress=CASE WHEN progress->>'phase' IN ('completed','failed','cancelled','denied') THEN progress ELSE jsonb_set(progress,'{phase}',to_jsonb($2::text)) END,updated_at=now() WHERE deployment_id=$1`, id, phase)
	return err
}

func UpdatePipelineProgress(ctx context.Context, tx Tx, progress PipelineProgress) error {
	if progress.InfrastructureApplied || progress.Phase == "outcome_unknown" {
		if err := RecordPipelineEffectEvidence(ctx, tx, progress.ID); err != nil {
			return err
		}
	}
	progress.LogReferences = nil
	progress.LogReferencesLimited = false
	raw, err := json.Marshal(progress)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(
		ctx,
		`UPDATE deployment_pipeline_runs SET progress=($2::jsonb - 'log_references' - 'log_references_limited') || jsonb_build_object('log_references',COALESCE(progress->'log_references','[]'::jsonb),'log_references_limited',COALESCE(progress->'log_references_limited','false'::jsonb)),site_revision=$3,updated_at=now()
 WHERE deployment_id=$1 AND site_revision<$3 AND (NOT settled OR $2::jsonb->>'phase' IN ('completed','failed','cancelled','denied'))`,
		progress.ID,
		raw,
		progress.Revision,
	)
	return err
}

func RunnerSupportsPipeline(configuration any) (bool, error) {
	return runnerSupportsProtocol(configuration, PipelineProtocol)
}

func RunnerSupportsPlanApply(configuration any) (bool, error) {
	return runnerSupportsProtocol(configuration, PipelinePlanApplyProtocol)
}

func runnerSupportsProtocol(configuration any, expected string) (bool, error) {
	raw, err := json.Marshal(configuration)
	if err != nil {
		return false, err
	}
	var c struct {
		SupportedProtocols []string `json:"supported_protocols"`
	}
	if err = json.Unmarshal(raw, &c); err != nil {
		return false, err
	}
	for _, protocol := range c.SupportedProtocols {
		if protocol == expected {
			return true, nil
		}
	}
	return false, nil
}
