package api

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/bundles"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

const pipelineCommandTTL = 24 * time.Hour

const (
	defaultNativePlanSeconds  = 1800
	defaultNativeApplySeconds = 7200
	defaultNativeTaskSeconds  = 1800
)

type PipelineDispatcher struct {
	NativeLimits *model.PipelineExecutionLimits
	Server       *Server
	Publisher    hmessaging.Publisher
	Key          ed25519.PrivateKey
	ToolsDigest  string
	MetadataKey  string
}

type pipelineExecutionRequest struct {
	RunnerUUID                string          `json:"runner_uuid,omitempty"`
	ID                        string          `json:"id"`
	OrgID                     string          `json:"org_id"`
	RunnerID                  string          `json:"runner_id"`
	EnvironmentID             string          `json:"environment_id"`
	Bundle                    []byte          `json:"bundle"`
	BundleSHA256              string          `json:"bundle_sha256"`
	Mode                      string          `json:"mode"`
	Backend                   string          `json:"backend"`
	Tool                      string          `json:"tool"`
	Manifest                  json.RawMessage `json:"manifest"`
	EncryptedLogsRecipient    string          `json:"encrypted_logs_recipient,omitempty"`
	EncryptedOutputsRecipient string          `json:"encrypted_outputs_recipient"`
	MetadataKey               string          `json:"metadata_key"`
	DeploymentToken           string          `json:"deployment_token,omitempty"`
}

type pipelineRecipeReference struct {
	ID      string `json:"id"`
	Version int    `json:"version"`
	Digest  string `json:"digest"`
}
type pipelineCommand struct {
	ExecutionLimits      *model.PipelineExecutionLimits     `json:"execution_limits,omitempty"`
	NativeTarget         *cp.NativeExecutionTargetReference `json:"native_target,omitempty"`
	RunnerUUID           string                             `json:"runner_uuid,omitempty"`
	RequiredCapabilities []string                           `json:"required_capabilities,omitempty"`
	Recipe               *pipelineRecipeReference           `json:"recipe,omitempty"`
	Cost                 *model.PipelineCost                `json:"cost,omitempty"`
	PlanBase             *pipelinePlanBase                  `json:"plan_base,omitempty"`
	SourcePlan           *pipelineSourcePlan                `json:"source_plan,omitempty"`
	Version              string                             `json:"version"`
	Strategy             string                             `json:"strategy"`
	ID                   string                             `json:"id"`
	OrgID                string                             `json:"org_id"`
	RunnerID             string                             `json:"runner_id"`
	EnvironmentID        string                             `json:"environment_id"`
	IssuedAt             time.Time                          `json:"issued_at"`
	ExpiresAt            time.Time                          `json:"expires_at"`
	Request              pipelineExecutionRequest           `json:"request"`
	Steps                []pipelineCommandStep              `json:"steps"`
	Approval             model.PipelineApproval             `json:"approval"`
	ToolsDigest          string                             `json:"tools_digest"`
}

type pipelinePlanBase struct {
	BaseDeploymentID string `json:"base_deployment_id"`
}
type pipelineSourcePlan struct {
	SourcePlanID     string `json:"source_plan_id"`
	Subject          string `json:"subject"`
	BaseDeploymentID string `json:"base_deployment_id"`
}

func NewPipelineDispatcher(server *Server, publisher hmessaging.Publisher, keyFile, digest, metadata string) (*PipelineDispatcher, error) {
	data, err := os.ReadFile(keyFile) // #nosec G304 -- operator-configured local signing key, never a request path.
	if err != nil {
		return nil, err
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(data)))
	digestBytes, digestErr := hex.DecodeString(digest)
	if err != nil || len(key) != ed25519.PrivateKeySize || digestErr != nil ||
		(digest == "" && server.RunnerRollouts != nil) || (digest != "" && len(digestBytes) != sha256.Size) {
		return nil, errors.New("invalid canonical pipeline signing configuration")
	}
	return &PipelineDispatcher{Server: server, Publisher: publisher, Key: key, ToolsDigest: digest, MetadataKey: metadata}, nil
}

func (p *PipelineDispatcher) Dispatch(ctx context.Context, dep *model.DeploymentSummary) error {
	if dep.NativeTarget != nil {
		if !dep.HasPipeline || dep.HasNativeObservation || dep.Mode == model.DeploymentModeObservation || !validNativeDispatchTarget(dep) {
			return errors.New("native execution target requires v2 dispatch for a deployment pipeline")
		}
	} else if p.Server.RunnerRollouts == nil || dep.OrgId != p.Server.RunnerRollouts.OrgID || dep.RunnerId != p.Server.RunnerRollouts.RunnerID {
		return errors.New("pipeline site is not configured")
	}
	owned, err := p.pipelineDispatchOwned(ctx, dep)
	if err != nil {
		return err
	}
	if !owned {
		if err = p.reserveCapacity(ctx, dep); err != nil {
			return err
		}
	}
	command, err := p.frozenCommand(ctx, dep)
	if err != nil {
		return err
	}
	var envelope hmessaging.CommandEnvelope
	if err = json.Unmarshal(command, &envelope); err != nil {
		return err
	}
	subject, err := hmessaging.RunnerCommandSubject(dep.OrgId, envelope.RunnerID)
	if err != nil {
		return err
	}
	id := envelope.CommandID
	if dep.NativeTarget != nil {
		id, err = hmessaging.NativeMessageID(dep.OrgId, envelope.RunnerID, "commands", envelope.Type, envelope.CommandID)
		if err != nil {
			return err
		}
	}
	return p.Publisher.Publish(ctx, hmessaging.Message{ID: id, Subject: subject, Data: command, CreatedAt: envelope.CreatedAt, ExpiresAt: envelope.ExpiresAt})
}

func validNativeDispatchTarget(dep *model.DeploymentSummary) bool {
	target := dep.NativeTarget
	return target.RunnerId == dep.RunnerId && nativeRunnerID.MatchString(target.RunnerId) && target.RunnerUuid != uuid.Nil &&
		target.RuntimeVersion.Valid() && target.PipelineVersion.Valid() && target.StateBackendType.Valid() &&
		nativeToolsDigest.MatchString(target.ToolsDigest) && nativeToolsDigest.MatchString(target.StateConfigurationDigest)
}

// This read is a hint only. The final transaction rechecks immutable command
// authority, Deployment revision and active ownership before returning bytes.
func (p *PipelineDispatcher) pipelineDispatchOwned(ctx context.Context, dep *model.DeploymentSummary) (bool, error) {
	tx, err := p.Server.Database.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var command []byte
	var owned bool
	if err = tx.QueryRowContext(ctx, `SELECT p.command,
 EXISTS(SELECT 1 FROM runner_job_reservations r WHERE r.deployment_id=d.id AND r.org_id=e.org_id AND r.runner_id=d.runner_id AND r.released_at IS NULL)
 FROM deployment_pipeline_runs p JOIN deployments d ON d.id=p.deployment_id JOIN deployment_environments e ON e.id=d.de_id
 WHERE d.id=$1 AND e.org_id=$2 AND d.runner_id=$3 AND e.id=$4`, dep.Id, dep.OrgId, dep.RunnerId, dep.DeploymentEnvUuid).Scan(&command, &owned); err != nil {
		return false, err
	}
	if len(command) != 0 {
		if _, err = model.FrozenPipelinePayload(command, dep); err != nil {
			return false, err
		}
		if !owned {
			return false, model.NewErrConflict("frozen pipeline command has no active Runner ownership")
		}
	}
	return owned, nil
}

func (p *PipelineDispatcher) reserveCapacity(ctx context.Context, dep *model.DeploymentSummary) error {
	runner, err := p.Server.ControlPlaneClient.GetInternalRunnerWithResponse(ctx, dep.OrgId, dep.RunnerId)
	if err != nil || runner.JSON200 == nil {
		return errors.New("pipeline Runner registration is unavailable")
	}
	limit, _, err := model.RunnerAdmissionConfiguration(runner.JSON200.RunnerConfiguration)
	if err != nil {
		return err
	}
	if dep.NativeTarget != nil && (runner.JSON200.OrgId != dep.OrgId || runner.JSON200.Id != dep.RunnerId || runner.JSON200.Uuid == nil ||
		*runner.JSON200.Uuid != dep.NativeTarget.RunnerUuid || limit <= 0) {
		return model.NewErrConflict("Native Runner capacity registration changed or is unavailable")
	}
	return p.Server.Database.ReserveRunnerDispatch(ctx, dep, limit)
}

func (p *PipelineDispatcher) frozenCommand(ctx context.Context, dep *model.DeploymentSummary) ([]byte, error) {
	tx, err := p.Server.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err = lockPipelineDispatchDeployment(ctx, tx, dep); err != nil {
		return nil, err
	}
	var existing []byte
	var backend string
	var definition model.PipelineDefinition
	var encodedDefinition json.RawMessage
	if err = tx.QueryRowContext(ctx, `SELECT command,backend,definition FROM deployment_pipeline_runs WHERE deployment_id=$1 FOR UPDATE`, dep.Id).Scan(&existing, &backend, &encodedDefinition); err != nil {
		return nil, err
	}
	if err = verifyPipelineDispatchOwnership(ctx, tx, dep, len(existing) != 0); err != nil {
		return nil, err
	}
	if dep.NativeTarget != nil && backend != string(dep.NativeTarget.StateBackendType) {
		return nil, model.NewErrConflict("pipeline backend does not match accepted Native target")
	}
	if len(existing) > 0 {
		if _, err = model.FrozenPipelinePayload(existing, dep); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if err = json.Unmarshal(encodedDefinition, &definition); err != nil {
		return nil, err
	}
	envelope, err := p.buildCommand(ctx, tx, dep, definition, backend)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET command=$2,updated_at=now() WHERE deployment_id=$1`, dep.Id, envelope); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now(),deadline=now()+interval '24 hours' WHERE deployment_id=$1`, dep.Id); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("freeze pipeline command: %w", err)
	}
	return envelope, nil
}

func lockPipelineDispatchDeployment(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary) error {
	var revision int
	var status string
	var targetJSON []byte
	if err := tx.QueryRowContext(ctx, `SELECT d.revision,d.status,d.native_target FROM deployments d JOIN deployment_environments e ON e.id=d.de_id
 WHERE d.id=$1 AND e.org_id=$2 AND d.runner_id=$3 AND e.id=$4 AND d.mode=$5 AND COALESCE(d.encrypted_outputs_recipient,'')=$6 FOR UPDATE OF d`,
		dep.Id, dep.OrgId, dep.RunnerId, dep.DeploymentEnvUuid, dep.Mode, dep.EncryptedOutputsRecipient.Or("")).Scan(&revision, &status, &targetJSON); err != nil {
		return err
	}
	var target *cp.NativeExecutionTargetReference
	if targetJSON != nil {
		if err := json.Unmarshal(targetJSON, &target); err != nil {
			return err
		}
	}
	if revision != dep.Revision || status != string(model.DeploymentStatusExecuting) || !sameCommandTarget(target, dep.NativeTarget) {
		return model.NewErrConflict("deployment changed before pipeline dispatch")
	}
	return nil
}

func verifyPipelineDispatchOwnership(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, frozen bool) error {
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runner_job_reservations WHERE deployment_id=$1 AND org_id=$2 AND runner_id=$3 AND released_at IS NULL)`, dep.Id, dep.OrgId, dep.RunnerId).Scan(&active); err != nil {
		return err
	}
	if !active {
		return model.NewErrConflict("pipeline dispatch requires active Runner ownership")
	}
	budget, err := model.ReadDeploymentDispatchBudget(ctx, tx, dep.Id)
	if err != nil {
		return err
	}
	if budget.Revision != dep.Revision || budget.Legacy || budget.CompletionReceivedAt.Valid ||
		budget.FirstAttemptAt.Valid != frozen || budget.Deadline.Valid != frozen || (frozen && !time.Now().Before(budget.Deadline.Time)) {
		return model.NewErrConflict("pipeline dispatch budget is exhausted or inconsistent")
	}
	return nil
}

func (p *PipelineDispatcher) buildCommand(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, definition model.PipelineDefinition, backend string) ([]byte, error) {
	logsRecipient := ""
	if dep.NativeTarget != nil && dep.Mode != model.DeploymentModeObservation {
		material, err := model.GetDeploymentLogMaterial(ctx, tx, dep.OrgId, dep.Id)
		if err != nil {
			if _, missing := model.IsErrNotFound(err); !missing {
				return nil, err
			}
		} else {
			logsRecipient = material.Recipient
		}
	}
	base, source, err := commandPlanAuthority(ctx, tx, dep)
	if err != nil {
		return nil, err
	}
	if source != nil {
		original, err := loadSavedPlanCommand(ctx, tx, p.Server.Database, dep, source)
		if err != nil {
			return nil, err
		}
		return p.signCommand(dep, definition, original.Request.Bundle, original.Request.Manifest, backend, nil, source, logsRecipient)
	}
	archive, err := bundles.Build(ctx, p.Server.Database, p.Server.ControlPlaneClient, dep.OrgId, dep.Id, p.Server.DeploymentAdmissions.ExecutionCatalogueClient())
	if err != nil {
		return nil, err
	}
	if archive.Len() > 512<<10 {
		return nil, model.NewErrBadRequest("pipeline bundle exceeds the qualified 512 KiB transport limit")
	}
	_, manifest, _, _, err := p.Server.Database.GetDeployment(ctx, tx, dep.OrgId, dep.Id, model.GetModeDefault)
	if err != nil {
		return nil, err
	}
	var input map[string]any
	if err = json.Unmarshal(manifest, &input); err != nil {
		return nil, err
	}
	input["environment_id"] = dep.DeploymentEnvUuid.String()
	taskManifest, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	return p.signCommand(dep, definition, archive.Bytes(), taskManifest, backend, base, source, logsRecipient)
}

func loadSavedPlanCommand(ctx context.Context, tx model.Tx, database model.Databaser, dep *model.DeploymentSummary, source *pipelineSourcePlan) (*pipelineCommand, error) {
	owner := dep
	if dep.Id.String() != source.SourcePlanID {
		id, err := uuid.Parse(source.SourcePlanID)
		if err != nil {
			return nil, errors.New("saved-plan command unavailable")
		}
		owner, _, _, _, err = database.GetDeployment(ctx, tx, dep.OrgId, id, model.GetModeDefault)
		if err != nil {
			return nil, err
		}
	}
	if !sameCommandTarget(owner.NativeTarget, dep.NativeTarget) {
		return nil, errors.New("saved-plan command target changed")
	}
	var raw, capture []byte
	if err := tx.QueryRowContext(ctx, `SELECT command,plan_capture FROM deployment_pipeline_runs WHERE deployment_id=$1`, source.SourcePlanID).Scan(&raw, &capture); err != nil {
		return nil, err
	}
	var saved pipelinePlanCapture
	if len(capture) == 0 || json.Unmarshal(capture, &saved) != nil || !sameCommandTarget(owner.NativeTarget, saved.NativeTarget) {
		return nil, errors.New("saved-plan command target changed")
	}
	payload, err := model.FrozenPipelinePayload(raw, owner)
	if err != nil {
		return nil, err
	}
	var original pipelineCommand
	if json.Unmarshal(payload, &original) != nil {
		return nil, errors.New("saved-plan command unavailable")
	}
	if original.Strategy != model.PipelinePlan || original.ID != source.SourcePlanID || original.OrgID != dep.OrgId ||
		original.EnvironmentID != dep.DeploymentEnvUuid.String() || original.RunnerID != dep.RunnerId ||
		original.PlanBase == nil || original.PlanBase.BaseDeploymentID != source.BaseDeploymentID {
		return nil, errors.New("saved-plan command scope changed")
	}
	return &original, nil
}

type pipelineCommandStep struct {
	ID         string          `json:"id"`
	ToolID     string          `json:"tool_id"`
	Parameters json.RawMessage `json:"parameters"`
}

func wirePipelineSteps(steps []model.PipelineStep) []pipelineCommandStep {
	result := make([]pipelineCommandStep, len(steps))
	for i, step := range steps {
		result[i] = pipelineCommandStep{ID: step.ID, ToolID: step.ToolID, Parameters: step.Parameters}
	}
	return result
}

func (p *PipelineDispatcher) signCommand(dep *model.DeploymentSummary, definition model.PipelineDefinition, archive []byte, taskManifest json.RawMessage, backend string, base *pipelinePlanBase, source *pipelineSourcePlan, logsRecipient string) ([]byte, error) {
	if definition.PipelineID != "" && !slices.Contains(definition.RequiredCapabilities, "pipeline.recipe.v1") {
		return nil, model.NewErrBadRequest("published pipeline capability requirements are not qualified")
	}
	if definition.Cost != nil && definition.Cost.Enabled && definition.Cost.PolicyRevision < 1 {
		return nil, model.NewErrConflict("cost estimation activation policy is not frozen")
	}
	mode := "deploy"
	if definition.Strategy == nativeObservationStrategy {
		mode = string(model.DeploymentModeObservation)
	}
	if dep.Mode == model.DeploymentModeRollback {
		mode = "rollback"
	}
	if definition.Strategy == model.PipelinePlan {
		mode = "plan_only"
	}
	if definition.Strategy == "destroy" {
		mode = "destroy"
	}
	version, tools, metadata, token, runnerUUID, route := model.PipelineProtocol, p.ToolsDigest, p.MetadataKey, "", "", dep.RunnerId
	if dep.NativeTarget == nil {
		token = util.GenerateHashedRunnerToken(p.Server.RunnerTokenSalt, dep.OrgId, dep.Id.String())
	} else {
		version, tools, metadata, runnerUUID = string(dep.NativeTarget.PipelineVersion), dep.NativeTarget.ToolsDigest, "", dep.NativeTarget.RunnerUuid.String()
		var err error
		route, err = hmessaging.NativeRunnerToken(runnerUUID)
		if err != nil {
			return nil, err
		}
	}
	now := time.Now().UTC()
	sum := sha256.Sum256(archive)
	cmd := pipelineCommand{
		PlanBase: base, SourcePlan: source,
		NativeTarget: dep.NativeTarget, RunnerUUID: runnerUUID,
		Version:              version,
		Strategy:             definition.Strategy,
		ID:                   dep.Id.String(),
		OrgID:                dep.OrgId,
		RunnerID:             dep.RunnerId,
		EnvironmentID:        dep.DeploymentEnvUuid.String(),
		IssuedAt:             now,
		ExpiresAt:            now.Add(pipelineCommandTTL),
		Steps:                wirePipelineSteps(definition.Steps),
		ToolsDigest:          tools,
		RequiredCapabilities: definition.RequiredCapabilities,
		Request: pipelineExecutionRequest{
			ID:                        dep.Id.String(),
			OrgID:                     dep.OrgId,
			RunnerID:                  dep.RunnerId,
			EnvironmentID:             dep.DeploymentEnvUuid.String(),
			Bundle:                    archive,
			BundleSHA256:              hex.EncodeToString(sum[:]),
			Mode:                      mode,
			Backend:                   backend,
			Tool:                      "opentofu",
			Manifest:                  taskManifest,
			EncryptedOutputsRecipient: dep.EncryptedOutputsRecipient.Or(""),
			RunnerUUID:                runnerUUID,
			MetadataKey:               metadata,
			DeploymentToken:           token,
		},
	}

	if dep.NativeTarget != nil {
		limits := p.NativeLimits
		if limits == nil {
			limits = &model.PipelineExecutionLimits{PlanSeconds: defaultNativePlanSeconds, ApplySeconds: defaultNativeApplySeconds, TaskSeconds: defaultNativeTaskSeconds}
		}
		if !limits.Valid() {
			return nil, model.NewErrConflict("Native execution limits are unavailable")
		}
		frozen := *limits
		cmd.ExecutionLimits = &frozen
	}

	if dep.NativeTarget != nil && logsRecipient != "" {
		cmd.Request.EncryptedLogsRecipient = logsRecipient
	}
	if definition.Strategy == model.PipelineReviewed || definition.IntendedStrategy == model.PipelineReviewed {
		cmd.Approval.Mode = "local"
	}
	if definition.Approval != nil {
		cmd.Approval = *definition.Approval
	}
	if definition.Strategy == nativeObservationStrategy {
		cmd.Request.MetadataKey = ""
		cmd.Request.EncryptedOutputsRecipient = ""
	}
	if definition.PipelineID != "" {
		cmd.Recipe = &pipelineRecipeReference{ID: definition.PipelineID, Version: definition.Version, Digest: definition.Digest}
		cmd.Cost = definition.Cost
		if cmd.Approval.Mode != "" {
			cmd.Approval.PolicyID = definition.PipelineID
			cmd.Approval.PolicyVersion = strconv.Itoa(definition.Version)
		}
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	signed, err := json.Marshal(struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}{payload, base64.StdEncoding.EncodeToString(ed25519.Sign(p.Key, payload))})
	if err != nil {
		return nil, err
	}
	envelope, err := json.Marshal(hmessaging.CommandEnvelope{
		ProtocolVersion: hmessaging.ProtocolVersionV1,
		CommandID:       "pipeline:" + dep.Id.String(),
		OrganizationID:  dep.OrgId,
		RunnerID:        route,
		DeploymentID:    dep.Id.String(),
		Type:            "execute-rollout-v1",
		CreatedAt:       now,
		ExpiresAt:       cmd.ExpiresAt,
		Payload:         signed,
	})
	if err != nil {
		return nil, err
	}
	if dep.NativeTarget != nil {
		if _, err = model.FrozenPipelinePayload(envelope, dep); err != nil {
			return nil, err
		}
	}
	return envelope, nil
}

func sameCommandTarget(a, b *cp.NativeExecutionTargetReference) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}
