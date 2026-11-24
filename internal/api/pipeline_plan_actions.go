package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genevents"
)

type pipelinePlanSource struct {
	Deployment *model.DeploymentSummary
	Run        *model.DeploymentPipeline
	Capture    *pipelinePlanCapture
	Backend    string
	Manifest   model.EncodedDeploymentManifest
	Tofu       model.RawTofu
	Graph      model.EncodedDeploymentGraph
}

func (s *Server) loadPipelinePlan(ctx context.Context, tx model.Tx, org string, id, actor uuid.UUID) (*pipelinePlanSource, error) {
	dep, _, _, _, err := s.Database.GetDeployment(ctx, tx, org, id, model.GetModeDefault)
	if err != nil {
		return nil, err
	}
	if !dep.HasPipeline {
		return nil, model.NewErrNotFound("pipeline deployment not found")
	}
	for _, permission := range []string{PermissionDeploymentRead, PermissionDeploymentWrite} {
		if err = s.checkEnvAuthorization(ctx, actor, org, dep.DeploymentEnvUuid, permission); err != nil {
			return nil, err
		}
	}
	source, err := s.lockManualPipelinePlan(ctx, tx, dep)
	if err != nil {
		return nil, err
	}
	capture, backend, err := readPipelinePlanCapture(ctx, tx, id, true)
	if err != nil {
		return nil, err
	}
	run, err := model.ReadDeploymentPipeline(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if capture == nil {
		return nil, model.NewErrConflict("This deployment has no captured manual Plan. Run a new manual Plan.")
	}
	if !reflect.DeepEqual(source.Deployment.NativeTarget, capture.NativeTarget) {
		return nil, model.NewErrConflict("saved-plan captured execution target is inconsistent")
	}
	source.Run, source.Capture, source.Backend = run, capture, backend
	return source, nil
}

// Match normal Deployment/result lock order before locking saved-plan metadata.
func (s *Server) lockManualPipelinePlan(ctx context.Context, tx model.Tx, initial *model.DeploymentSummary) (*pipelinePlanSource, error) {
	if err := model.LockEnvironmentMutation(ctx, tx, initial.DeploymentEnvUuid); err != nil {
		return nil, err
	}
	dep, manifest, tofu, graph, err := s.Database.GetDeployment(ctx, tx, initial.OrgId, initial.Id, model.GetModeForUpdate)
	if err != nil {
		return nil, err
	}
	if dep.Mode != model.DeploymentModeDeployPlan || dep.DeploymentEnvUuid != initial.DeploymentEnvUuid {
		return nil, model.NewErrConflict("This deployment is not the captured manual Plan.")
	}
	return &pipelinePlanSource{Deployment: dep, Manifest: manifest, Tofu: tofu, Graph: graph}, nil
}

func pipelineActionKey(action string, id uuid.UUID, key string) string {
	return "pipeline-" + action + ":" + id.String() + ":" + key
}

func (s *Server) applyPipelinePlan(ctx context.Context, actor uuid.UUID, org string, id uuid.UUID, subject, key string) (_ *model.DeploymentSummary, resultErr error) {
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	source, err := s.loadPipelinePlan(ctx, tx, org, id, actor)
	if err != nil {
		return nil, err
	}
	scopedKey := pipelineActionKey("apply", id, key)
	digest := digestIdempotencyKey(scopedKey)
	child, boundSubject, err := readPipelineApply(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if child.Valid {
		return s.recoverPipelineApply(ctx, tx, source, child.UUID, actor, subject, boundSubject, digest)
	}
	if err = s.validatePipelineApply(ctx, tx, source, subject); err != nil {
		return nil, err
	}
	if s.DeploymentAdmissions == nil {
		return nil, ErrDeploymentAdmissionUnavailable
	}
	env, err := s.savedApplyTarget(ctx, source)
	if err != nil {
		return nil, err
	}
	if err := model.RequireStateCustody(ctx, tx, org, source.Deployment.ProjectId, source.Deployment.EnvId, source.Deployment.DeploymentEnvUuid, source.Deployment.RunnerId, source.Deployment.NativeTarget); err != nil {
		return nil, err
	}
	admission := s.DeploymentAdmissions.NewAttempt(org, *env.ProjectUuid, source.Deployment.DeploymentEnvUuid, actor, model.DeploymentModeDeploy, scopedKey)
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
			resultErr = sealRetainedAdmission(ctx, admission, resultErr)
		}
	}()
	intent := ordinaryDeploymentCompiledIntent{NativeTarget: source.Deployment.NativeTarget, Pipeline: savedPipelineDefinition(source), ProjectID: source.Deployment.ProjectId, EnvironmentID: source.Deployment.EnvId, RunnerID: source.Deployment.RunnerId,
		EffectiveBaseDeploymentID: source.Capture.BaseDeploymentID, Manifest: source.Manifest, Graph: source.Graph, Tofu: source.Tofu,
		EncryptedOutputsRecipient: source.Deployment.EncryptedOutputsRecipient.Or(""), ManagedLogs: true, RunnerLogLevel: source.Deployment.RunnerLogLevel,
		CarryForwardModuleVersions: source.Capture.Lifecycle.CarryForward, ConfirmedRestrictedVersionUUIDs: source.Capture.Lifecycle.Confirmed, RetainedSubject: subject}
	if err = admission.Authorize(ctx, tx, source.Deployment.ModuleArtifacts, intent); err != nil {
		return nil, err
	}
	dep, messages, err := s.createPipelineApply(ctx, tx, source, admission, actor, subject, digest)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	reliableoutbox.OptimisticPublish(ctx, s.Logger, s.Database.AsReliableOutboxStore(), s.EventPublisher, messages)
	return dep, nil
}

func (s *Server) savedApplyTarget(ctx context.Context, source *pipelinePlanSource) (*cp.Environment, error) {
	env, err := s.retainedTarget(ctx, source.Deployment.OrgId, source.Deployment.ProjectId, source.Deployment.EnvId, source.Deployment.DeploymentEnvUuid, source.Deployment.RunnerId)
	if err != nil {
		return nil, err
	}
	runner, err := s.ControlPlaneClient.GetRunnerWithResponse(ctx, source.Deployment.OrgId, source.Deployment.RunnerId)
	if err != nil {
		return nil, err
	}
	if runner.StatusCode() != http.StatusOK || runner.JSON200 == nil || env.ProjectUuid == nil {
		return nil, model.NewErrConflict("saved-plan Runner unavailable")
	}
	if runner.JSON200.OrgId != source.Deployment.OrgId || runner.JSON200.Id != source.Deployment.RunnerId {
		return nil, model.NewErrConflict("saved-plan Runner identity changed")
	}
	var pinned *model.PipelineDefinition
	if source.Run.Definition.PipelineID != "" {
		pinned = &source.Run.Definition
	}
	target, err := nativeExecutionTarget(source.Deployment.OrgId, source.Deployment.RunnerId, *runner.JSON200, model.DeploymentModeDeploy, pinned)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(source.Deployment.NativeTarget, target) {
		return nil, model.NewErrConflict("saved-plan execution target changed")
	}
	if target != nil {
		if err := requireNativeFirstEnvironment(*env); err != nil {
			return nil, err
		}
	} else {
		supported, err := model.RunnerSupportsPlanApply(runner.JSON200.RunnerConfiguration)
		if err != nil {
			return nil, err
		}
		if !supported {
			return nil, model.NewErrConflict("Runner does not support saved-plan Apply")
		}
	}
	definition, err := pipelineForRunner(*runner.JSON200, model.DeploymentModeDeploy, pinned)
	if err != nil {
		return nil, err
	}
	storage, err := json.Marshal(runner.JSON200.StateStorageConfiguration)
	if err != nil {
		return nil, err
	}
	if definition == nil || hashRetained(storage) != source.Capture.StorageSHA256 {
		return nil, model.NewErrConflict("saved-plan execution target changed")
	}
	return env, nil
}

func (s *Server) validatePipelineApply(ctx context.Context, tx model.Tx, source *pipelinePlanSource, subject string) error {
	if subject == "" || source.Run.Progress.Subject != subject {
		return model.NewErrConflict("saved-plan subject changed")
	}
	state, err := s.pipelinePlanStatus(ctx, tx, source.Deployment, source.Run)
	if err != nil {
		return err
	}
	if state.State != pipelinePlanReady {
		if state.State == pipelinePlanSuperseded {
			return model.NewErrCodedConflict(model.CodeDeploymentBaseChanged, "This Plan is out of date. Create a new plan.").AboutDeployment(*state.CurrentDeploymentID)
		}
		return model.NewErrConflict(state.Reason)
	}
	digest, err := pipelineMaterialDigest(source.Manifest, source.Graph, source.Tofu, source.Deployment.ModuleArtifacts, source.Backend)
	if err != nil {
		return err
	}
	if digest != source.Capture.MaterialSHA256 {
		return model.NewErrConflict("saved-plan source material changed")
	}
	command, err := loadSavedPlanCommand(ctx, tx, s.Database, source.Deployment, &pipelineSourcePlan{SourcePlanID: source.Deployment.Id.String(), Subject: subject, BaseDeploymentID: source.Capture.BaseDeploymentID.String()})
	if err != nil {
		return model.NewErrConflict("Saved-plan command is unavailable. Create a new plan.")
	}
	if source.Deployment.NativeTarget == nil && s.PipelineDispatcher != nil && (command.ToolsDigest != s.PipelineDispatcher.ToolsDigest || command.Request.MetadataKey != s.PipelineDispatcher.MetadataKey) {
		return model.NewErrConflict("Runner tooling changed. Create a new plan.")
	}
	return nil
}

func (s *Server) recoverPipelineApply(ctx context.Context, tx model.Tx, source *pipelinePlanSource, id, actor uuid.UUID, subject, boundSubject, keyDigest string) (*model.DeploymentSummary, error) {
	var savedDigest sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT idempotency_key_digest FROM deployments WHERE id=$1`, id).Scan(&savedDigest); err != nil {
		return nil, err
	}
	dep, _, _, _, err := s.Database.GetDeployment(ctx, tx, source.Deployment.OrgId, id, model.GetModeDefault)
	if err != nil {
		return nil, err
	}
	if !savedDigest.Valid || savedDigest.String != keyDigest || boundSubject != subject || dep.CreatedBy != actor {
		return nil, model.NewErrConflict("This Plan is already used by another Apply request.")
	}
	return dep, nil
}

func (s *Server) createPipelineApply(ctx context.Context, tx model.Tx, source *pipelinePlanSource, admission *OrdinaryDeploymentAdmissionAttempt, actor uuid.UUID, subject, keyDigest string) (*model.DeploymentSummary, []*hstandardoutbox.PendingEventMessage, error) {
	logMaterial, err := (DeploymentLogMaterialService{Vault: s.Vault}).Generate(ctx)
	if err != nil {
		return nil, nil, err
	}
	expected := source.Capture.BaseDeploymentID
	if expected == uuid.Nil {
		var err error
		expected, err = model.EffectiveStateDeploymentID(ctx, tx, source.Deployment.DeploymentEnvUuid)
		if err != nil {
			return nil, nil, err
		}
	}
	definition := savedPipelineDefinition(source)
	original := source.Deployment
	dep, err := s.Database.CreateDeployment(ctx, tx, original.OrgId, original.ProjectId, original.EnvId, model.CreateDeploymentParams{
		NativeTarget: original.NativeTarget, CreatedBy: actor, DeploymentEnvUuid: original.DeploymentEnvUuid, Mode: model.DeploymentModeDeploy, Manifest: source.Manifest, Graph: source.Graph, Tofu: source.Tofu,
		RunnerId: original.RunnerId, EncryptedOutputsRecipient: original.EncryptedOutputsRecipient, Metrics: original.Metrics, RunnerLogLevel: original.RunnerLogLevel,
		EncryptedLogsRecipient: opt.Of(logMaterial.Recipient),
		ModuleArtifacts:        original.ModuleArtifacts, ModuleVersionRequestDigest: original.ModuleVersionRequestDigest, ExpectedBaseDeploymentID: opt.Of(expected), IdempotencyKeyDigest: opt.Of(keyDigest),
		Pipeline: definition, PipelineBackend: source.Backend})
	if err != nil {
		return nil, nil, err
	}
	if err := model.StoreDeploymentLogMaterial(ctx, tx, original.OrgId, dep.Id, *logMaterial); err != nil {
		return nil, nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET source_plan_id=$2,source_plan_subject=$3 WHERE deployment_id=$1`, dep.Id, original.Id, subject); err != nil {
		return nil, nil, err
	}
	parsed, err := graphs.FromJson(bytes.NewReader(source.Graph))
	if err != nil {
		return nil, nil, err
	}
	if err = s.Database.InitActiveResourcesFromGraph(ctx, tx, original.DeploymentEnvUuid, dep.Id, parsed); err != nil {
		return nil, nil, err
	}
	if err = admission.Bind(ctx, tx, dep.Id); err != nil {
		return nil, nil, err
	}
	messages, err := s.Database.InsertPendingEventMessages(ctx, tx, []*hstandardoutbox.PendingEventMessage{{Subject: string(genevents.IoPlatformOrchestratorDeploymentCreated), Payload: model.ConvertDeploymentToEventPayload(dep)}})
	return dep, messages, err
}

func (s *Server) replanPipelinePlan(ctx context.Context, actor uuid.UUID, org string, id uuid.UUID, key string) (_ *model.DeploymentSummary, resultErr error) {
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	source, err := s.loadPipelinePlan(ctx, tx, org, id, actor)
	if err != nil {
		return nil, err
	}
	scopedKey := pipelineActionKey("replan", id, key)
	existing, err := s.recoverPipelineReplan(ctx, tx, source, actor, digestIdempotencyKey(scopedKey))
	if err != nil || existing != nil {
		return existing, err
	}
	state, err := s.pipelinePlanStatus(ctx, tx, source.Deployment, source.Run)
	if err != nil {
		return nil, err
	}
	if !state.CanReplan {
		return nil, model.NewErrConflict("This Plan cannot create another plan.")
	}
	response, err := s.ControlPlaneClient.GetEnvironmentWithResponse(ctx, org, source.Deployment.ProjectId, source.Deployment.EnvId)
	if err != nil {
		return nil, err
	}
	env := response.JSON200
	if response.StatusCode() != http.StatusOK || env == nil || env.ProjectUuid == nil || env.Uuid != source.Deployment.DeploymentEnvUuid ||
		env.ProjectId != source.Deployment.ProjectId || env.Id != source.Deployment.EnvId || env.Status == cp.EnvironmentStatusDeleting {
		return nil, model.NewErrConflict("replan Environment identity or lifecycle changed")
	}
	request := source.Capture.Request
	request.NativeTarget, request.runner = nil, nil
	selection := source.Run.Definition.RequestedSelection
	var requested *DeploymentPipelineSelection
	if selection != nil {
		requested = &DeploymentPipelineSelection{Id: selection.ID, Version: selection.Version}
	}
	request.Pipeline, request.NativeTarget, request.runner, err = s.resolveRequestedPipelineForRequest(ctx, actor, org, env, model.DeploymentModeDeployPlan, requested, nil)
	if err != nil {
		return nil, err
	}
	options := s.deploymentOptions(ctx, actor, org, *env, model.DeploymentModeDeployPlan, request, scopedKey, false)
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
			resultErr = sealRetainedAdmission(ctx, options.Admission, resultErr)
		}
	}()
	dep, messages, _, err := CreateDeploymentWithOptions(
		ctx, s.Logger, actor, org, source.Deployment.ProjectId, source.Deployment.EnvId, *env,
		model.DeploymentModeDeployPlan, opt.Empty[uuid.UUID](), *request.Manifest,
		request.EncryptedOutputsRecipient, nil, nil, false, request.RunnerLogLevel,
		s.ControlPlaneClient, s.Database, tx, options)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET replanned_from_id=$2 WHERE deployment_id=$1`, dep.Id, id); err != nil {
		return nil, err
	}
	// The locked parent lineage owns retry identity without another command-key lock.
	if _, err = tx.ExecContext(ctx, `UPDATE deployments SET idempotency_key_digest=$2 WHERE id=$1`, dep.Id, digestIdempotencyKey(scopedKey)); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	reliableoutbox.OptimisticPublish(ctx, s.Logger, s.Database.AsReliableOutboxStore(), s.EventPublisher, messages)
	return dep, nil
}

func (s *Server) recoverPipelineReplan(ctx context.Context, tx model.Tx, source *pipelinePlanSource, actor uuid.UUID, keyDigest string) (*model.DeploymentSummary, error) {
	var existing uuid.UUID
	err := tx.QueryRowContext(ctx, `SELECT d.id FROM deployment_pipeline_runs p JOIN deployments d ON d.id=p.deployment_id WHERE p.replanned_from_id=$1 AND d.idempotency_key_digest=$2`, source.Deployment.Id, keyDigest).Scan(&existing)
	if err == nil {
		dep, _, _, _, loadErr := s.Database.GetDeployment(ctx, tx, source.Deployment.OrgId, existing, model.GetModeDefault)
		if loadErr == nil && dep.CreatedBy != actor {
			return nil, model.NewErrConflict("re-plan request actor changed")
		}
		return dep, loadErr
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return nil, err
}

func savedPipelineDefinition(source *pipelinePlanSource) *model.PipelineDefinition {
	if source.Run.Definition.PipelineID != "" {
		definition := source.Run.Definition
		definition.Strategy = model.PipelineApplySaved
		return &definition
	}
	return &model.PipelineDefinition{ID: "apply-plan", Version: 1, Name: "Apply saved plan", Strategy: model.PipelineApplySaved, Steps: []model.PipelineStep{}}
}
