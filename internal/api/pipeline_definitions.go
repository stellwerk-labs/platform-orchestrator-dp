package api

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"slices"
	"strconv"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func publishedPipeline(version p.PublishedPipelineVersion, mode model.DeploymentMode) (*model.PipelineDefinition, error) {
	if version.Id == uuid.Nil || version.PipelineId == uuid.Nil || version.RetiredAt != nil {
		return nil, model.NewErrBadRequest("published pipeline version is unavailable")
	}
	digest, err := p.RecipeDigest(version.Name, version.Definition)
	if err != nil || digest != version.Digest {
		return nil, model.NewErrBadRequest("published pipeline digest is invalid")
	}
	definition := &model.PipelineDefinition{ID: version.Id.String(), Version: version.Version, Name: version.Name, PipelineID: version.PipelineId.String(), ProjectID: version.ProjectId, Digest: version.Digest, Strategy: string(version.Definition.Strategy), Steps: []model.PipelineStep{}}
	for _, step := range version.Definition.Steps {
		parameters, _ := json.Marshal(step.Parameters)
		definition.Steps = append(definition.Steps, model.PipelineStep{ID: step.Id, ToolID: string(step.ToolId), Phase: string(step.Phase), Parameters: parameters})
	}
	if version.Definition.Cost != nil {
		definition.Cost = &model.PipelineCost{Enabled: version.Definition.Cost.Enabled}
	}
	if version.Definition.Approval != nil {
		definition.Approval = &model.PipelineApproval{Mode: string(version.Definition.Approval.Mode), PolicyID: version.PipelineId.String(), PolicyVersion: strconv.Itoa(version.Version)}
	}
	if model.IsPlanDeployment(mode) {
		definition.IntendedStrategy = definition.Strategy
		definition.Strategy = model.PipelinePlan
	} else if mode != model.DeploymentModeDeploy && mode != model.DeploymentModeRollback {
		return nil, model.NewErrConflict("published project pipelines currently support Deploy and manual Plan only; this operation must be qualified before project policy allows it")
	}
	return definition, nil
}

func validatePinnedRecipe(definition *model.PipelineDefinition) error {
	if definition.PipelineID == "" {
		return model.NewErrBadRequest("published pipeline provenance is missing")
	}
	strategy := definition.Strategy
	if strategy == model.PipelinePlan || strategy == model.PipelineApplySaved {
		strategy = definition.IntendedStrategy
	}
	recipe := p.PipelineRecipe{Strategy: p.PipelineRecipeStrategy(strategy), Steps: []p.PipelineRecipeStep{}}
	for _, step := range definition.Steps {
		var parameters map[string]any
		if json.Unmarshal(step.Parameters, &parameters) != nil {
			return model.NewErrBadRequest("invalid pinned task parameters")
		}
		recipe.Steps = append(recipe.Steps, p.PipelineRecipeStep{Id: step.ID, ToolId: p.PipelineRecipeStepToolId(step.ToolID), Phase: p.PipelineRecipeStepPhase(step.Phase), Parameters: parameters})
	}
	if definition.Cost != nil {
		recipe.Cost = &struct {
			Enabled bool `json:"enabled"`
		}{definition.Cost.Enabled}
	}
	if definition.Approval != nil {
		recipe.Approval = &struct {
			Mode p.PipelineRecipeApprovalMode `json:"mode"`
		}{p.PipelineRecipeApprovalMode(definition.Approval.Mode)}
		if definition.Approval.PolicyID != definition.PipelineID || definition.Approval.PolicyVersion != strconv.Itoa(definition.Version) {
			return model.NewErrBadRequest("pinned approval policy changed")
		}
	}
	digest, err := p.RecipeDigest(definition.Name, recipe)
	if err != nil || digest != definition.Digest {
		return model.NewErrBadRequest("pinned published pipeline definition changed")
	}
	return nil
}

func pipelineForRunner(runner cp.Runner, mode model.DeploymentMode, pinned *model.PipelineDefinition) (*model.PipelineDefinition, error) {
	protocol := model.PipelineProtocol
	if kind, _ := runner.RunnerConfiguration.Discriminator(); kind == string(cp.RunnerTypeNativeAgent) {
		if _, err := nativeExecutionTarget(runner.OrgId, runner.Id, runner, mode, pinned); err != nil {
			return nil, err
		}
		protocol = string(cp.NativeTargetPipelineV2)
	}
	raw, err := json.Marshal(runner.RunnerConfiguration)
	if err != nil {
		return nil, err
	}
	var config struct {
		SupportedProtocols []string `json:"supported_protocols"`
		Capabilities       []string `json:"capabilities"`
	}
	if err = json.Unmarshal(raw, &config); err != nil {
		return nil, err
	}
	if !slices.Contains(config.SupportedProtocols, protocol) {
		if pinned != nil {
			return nil, model.NewErrBadRequest("selected Runner does not support deployment pipelines")
		}
		return nil, nil
	}
	if err := validateNativeJobConfiguration(runner.RunnerConfiguration); err != nil {
		return nil, err
	}

	if pinned != nil {
		if pinned.PipelineID != "" {
			if err := validatePinnedRecipe(pinned); err != nil {
				return nil, err
			}
			capabilities := append(slices.Clone(config.SupportedProtocols), config.Capabilities...)
			if err := qualifyRecipeCapabilities(pinned, capabilities); err != nil {
				return nil, err
			}
			return pinned, nil
		}
		expected, err := model.ResolvePipeline(&model.PipelineSelection{ID: pinned.ID, Version: pinned.Version}, mode)
		if err != nil {
			return nil, err
		}
		expected.RequestedSelection = pinned.RequestedSelection
		expected.RequestIdentityVersion = pinned.RequestIdentityVersion
		if !reflect.DeepEqual(expected, pinned) {
			return nil, model.NewErrBadRequest("pinned pipeline definition changed")
		}
		return pinned, nil
	}
	return model.ResolvePipeline(nil, mode)
}

func (s *Server) projectPipelineChoices(ctx context.Context, org string, env *cp.Environment) (*p.PipelineChoices, error) {
	if s.PipelineClient == nil {
		return nil, nil
	}
	response, err := s.PipelineClient.ListEnvironmentPipelineChoicesWithResponse(ctx, org, env.ProjectId, env.Id)
	if err != nil {
		return nil, err
	}
	if response.StatusCode() != http.StatusOK || response.JSON200 == nil {
		return nil, model.NewErrConflict("project pipeline policy is unavailable; retry after Core recovers")
	}
	return response.JSON200, nil
}

// An accepted request answers from its frozen recipe before consulting mutable defaults.
func (s *Server) recoverRequestedPipeline(ctx context.Context, actor uuid.UUID, org string, env *cp.Environment, mode model.DeploymentMode, key *string, selection *DeploymentPipelineSelection) (*model.PipelineDefinition, *cp.NativeExecutionTargetReference, bool, error) {
	if key == nil {
		return nil, nil, false, nil
	}
	var definition *model.PipelineDefinition
	var target *cp.NativeExecutionTargetReference
	var found bool
	err := s.inTx(ctx, func(tx model.Tx) error {
		var err error
		definition, target, found, err = s.readAcceptedPipeline(ctx, tx, org, env, digestIdempotencyKey(*key))
		if err != nil || found || s.DeploymentAdmissions == nil {
			return err
		}
		attempt := s.DeploymentAdmissions.NewAttempt(org, opt.OfRef(env.ProjectUuid).Or(uuid.Nil), env.Uuid, actor, mode, *key)
		receipt, err := model.GetDeploymentAdmissionReceipt(ctx, tx, org, attempt.AdmissionID)
		if _, missing := model.IsErrNotFound(err); missing {
			return nil
		}
		if err != nil {
			return err
		}
		if receipt.Disposition == "pending" {
			return ErrDeploymentAdmissionUnavailable
		}
		return model.NewErrConflict("the previous Deployment admission has a durable outcome; use a new request key")
	})
	if err != nil || !found {
		return nil, nil, found, err
	}
	if err = validateAcceptedPipelineSelection(definition, selection); err != nil {
		return nil, nil, false, err
	}
	return definition, target, true, nil
}
func (s *Server) readAcceptedPipeline(ctx context.Context, tx model.Tx, org string, env *cp.Environment, digest string) (*model.PipelineDefinition, *cp.NativeExecutionTargetReference, bool, error) {
	item, err := model.FindDeploymentQueueItemByIdempotencyKey(ctx, tx, org, env.Uuid, digest)
	if err == nil {
		var request deploymentRequest
		if err = json.Unmarshal(item.Intent, &request); err != nil {
			return nil, nil, false, err
		}
		return request.Pipeline, request.NativeTarget, true, nil
	}
	if _, missing := model.IsErrNotFound(err); !missing {
		return nil, nil, false, err
	}
	return s.readAcceptedDeploymentPipeline(ctx, tx, org, env, digest)
}
func (s *Server) readAcceptedDeploymentPipeline(ctx context.Context, tx model.Tx, org string, env *cp.Environment, digest string) (*model.PipelineDefinition, *cp.NativeExecutionTargetReference, bool, error) {
	dep, _, err := s.Database.GetDeploymentByIdempotencyKeyDigest(ctx, tx, org, env.ProjectId, env.Id, digest)
	if _, missing := model.IsErrNotFound(err); missing {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	if dep.DeploymentEnvUuid != env.Uuid {
		return nil, nil, false, model.NewErrConflict("accepted Deployment belongs to another Environment identity")
	}
	run, err := model.ReadDeploymentPipeline(ctx, tx, dep.Id)
	if err != nil {
		return nil, nil, false, err
	}
	if run == nil {
		return nil, dep.NativeTarget, true, nil
	}
	return &run.Definition, dep.NativeTarget, true, nil
}
func requestedPipelineSelection(selection *DeploymentPipelineSelection) *model.PipelineSelection {
	if selection == nil {
		return nil
	}
	return &model.PipelineSelection{ID: string(selection.Id), Version: int(selection.Version)}
}
func validateAcceptedPipelineSelection(definition *model.PipelineDefinition, selection *DeploymentPipelineSelection) error {
	if definition == nil {
		if selection != nil {
			return model.NewErrConflict("accepted request did not select a pipeline")
		}
		return nil
	}
	requested := requestedPipelineSelection(selection)
	if definition.RequestIdentityVersion > 0 {
		if !reflect.DeepEqual(requested, definition.RequestedSelection) {
			return model.NewErrConflict("pipeline selection differs from accepted idempotent request")
		}
		return nil
	}
	if requested != nil && (requested.ID != definition.ID || requested.Version != definition.Version) {
		return model.NewErrConflict("pipeline selection differs from accepted idempotent request")
	}
	return nil
}
func (s *Server) resolveRequestedPipeline(ctx context.Context, org string, env *cp.Environment, mode model.DeploymentMode, selection *DeploymentPipelineSelection) (*model.PipelineDefinition, *cp.NativeExecutionTargetReference, *cp.Runner, error) {
	return s.resolveRequestedPipelineWithRunner(ctx, org, env, mode, selection, nil)
}
func (s *Server) resolveRequestedPipelineWithRunner(ctx context.Context, org string, env *cp.Environment, mode model.DeploymentMode, selection *DeploymentPipelineSelection, runner *cp.Runner) (*model.PipelineDefinition, *cp.NativeExecutionTargetReference, *cp.Runner, error) {
	choices, err := s.projectPipelineChoices(ctx, org, env)
	if err != nil {
		return nil, nil, nil, err
	}
	requested := requestedPipelineSelection(selection)
	pinned, err := selectProjectPipeline(choices, requested, mode)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = s.bindRequestedPipelineCostPolicy(ctx, pinned); err != nil {
		return nil, nil, nil, err
	}
	if runner == nil {
		runner, err = s.requestedPipelineRunner(ctx, org, env)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	target, err := nativeExecutionTarget(org, *env.RunnerId, *runner, mode, pinned)
	if err != nil {
		return nil, nil, nil, err
	}
	if target != nil {
		if err := requireNativeFirstEnvironment(*env); err != nil {
			return nil, nil, nil, err
		}
	}
	definition, err := pipelineForRunner(*runner, mode, pinned)
	if err != nil || definition == nil {
		return definition, target, runner, err
	}
	definition.RequestedSelection = requested
	definition.RequestIdentityVersion = 1
	return definition, target, runner, nil
}
func (s *Server) resolveRequestedPipelineForRequest(ctx context.Context, actor uuid.UUID, org string, env *cp.Environment, mode model.DeploymentMode, selection *DeploymentPipelineSelection, key *string) (*model.PipelineDefinition, *cp.NativeExecutionTargetReference, *cp.Runner, error) {
	if frozen, target, found, err := s.recoverRequestedPipeline(ctx, actor, org, env, mode, key, selection); err != nil || found {
		return frozen, target, nil, err
	}
	if env.Status == cp.EnvironmentStatusDeleting && mode != model.DeploymentModeDestroy {
		return nil, nil, nil, model.NewErrCodedConflict(model.CodeEnvironmentDeleting, "environment is in status 'deleting'")
	}
	if s.RunnerRollouts != nil && org == s.RunnerRollouts.OrgID {
		return s.resolveRequestedPipeline(ctx, org, env, mode, selection)
	}
	if env.RunnerId == nil {
		if selection != nil {
			return nil, nil, nil, model.NewErrBadRequest("pipeline selection is not enabled for this organization")
		}
		return nil, nil, nil, nil
	}
	runner, err := s.requestedPipelineRunner(ctx, org, env)
	if err != nil {
		return nil, nil, nil, err
	}
	if kind, _ := runner.RunnerConfiguration.Discriminator(); kind == string(cp.RunnerTypeNativeAgent) {
		return s.resolveRequestedPipelineWithRunner(ctx, org, env, mode, selection, runner)
	}
	if selection != nil {
		return nil, nil, nil, model.NewErrBadRequest("pipeline selection is not enabled for this organization")
	}
	return nil, nil, runner, nil
}
func selectProjectPipeline(choices *p.PipelineChoices, requested *model.PipelineSelection, mode model.DeploymentMode) (*model.PipelineDefinition, error) {
	effective := requested
	if mode == model.DeploymentModeRollback && requested == nil && choices != nil && choices.Policy.Enabled {
		if choices.EffectiveRollbackDefault == nil || choices.EffectiveRollbackDefault.VersionId == nil {
			return nil, model.NewErrConflict("Rollback requires an explicit published pipeline selection or an available rollback default; forward defaults are not compensation")
		}
		effective = &model.PipelineSelection{ID: choices.EffectiveRollbackDefault.VersionId.String()}
	}
	if choices != nil && choices.Policy.Enabled && effective == nil {
		if choices.EffectiveDefault != nil && choices.EffectiveDefault.VersionId != nil {
			effective = &model.PipelineSelection{ID: choices.EffectiveDefault.VersionId.String()}
		} else if choices.Policy.DefaultVersionId == nil {
			return nil, model.NewErrConflict("project pipeline default unavailable")
		}
		if effective == nil {
			effective = &model.PipelineSelection{ID: choices.Policy.DefaultVersionId.String()}
		}
	}
	if effective != nil && choices != nil {
		if version := findPublishedPipelineChoice(choices, effective); version != nil {
			return publishedPipeline(*version, mode)
		}
	}
	if choices != nil && choices.Policy.Enabled {
		return nil, model.NewErrConflict("selected pipeline is not an allowed published version for this project")
	}
	if effective == nil {
		return nil, nil
	}
	return model.ResolvePipeline(effective, mode)
}
func findPublishedPipelineChoice(choices *p.PipelineChoices, selection *model.PipelineSelection) *p.PublishedPipelineVersion {
	for _, version := range choices.Versions {
		if version.Id.String() == selection.ID && (selection.Version == 0 || version.Version == selection.Version) {
			return &version
		}
	}
	return nil
}
func (s *Server) requestedPipelineRunner(ctx context.Context, org string, env *cp.Environment) (*cp.Runner, error) {
	if env.RunnerId == nil {
		response, err := s.ControlPlaneClient.UpdateRunnerInAnEnvironmentWithResponse(ctx, org, env.ProjectId, env.Id, &cp.UpdateRunnerInAnEnvironmentParams{})
		if err != nil {
			return nil, err
		}
		if response.JSON200 == nil {
			return nil, model.NewErrConflict("Runner selection unavailable")
		}
		env.RunnerId = &response.JSON200.RunnerId
	}
	response, err := s.ControlPlaneClient.GetRunnerWithResponse(ctx, org, *env.RunnerId)
	if err != nil {
		return nil, err
	}
	if response.StatusCode() != http.StatusOK || response.JSON200 == nil {
		return nil, model.NewErrConflict("Runner unavailable")
	}
	if response.JSON200.OrgId != org || response.JSON200.Id != *env.RunnerId {
		return nil, model.NewErrConflict("resolved Runner identity changed")
	}
	return response.JSON200, nil
}

func qualifyRecipeCapabilities(definition *model.PipelineDefinition, available []string) error {
	if !slices.Contains(available, "pipeline.recipe.v1") {
		return model.NewErrConflict("selected Runner does not support immutable project pipeline recipes")
	}
	required := []string{"pipeline.recipe.v1"}
	tools := map[string]p.PipelineTool{}
	for _, tool := range p.TrustedTools() {
		tools[tool.Id] = tool
	}
	for _, step := range definition.Steps {
		required = append(required, tools[step.ToolID].RequiredCapabilities...)
	}
	costRequirements, err := requiredPipelineCostCapabilities(definition, available)
	if err != nil {
		return err
	}
	required = append(required, costRequirements...)

	slices.Sort(required)
	required = slices.Compact(required)
	for _, capability := range required {
		if !slices.Contains(available, capability) {
			return model.NewErrConflict("selected Runner lacks task capability: " + capability)
		}
	}
	if len(definition.RequiredCapabilities) > 0 && !slices.Equal(definition.RequiredCapabilities, required) {
		return model.NewErrConflict("accepted pipeline capability requirements changed")
	}
	definition.RequiredCapabilities = required
	return nil
}

func requiredPipelineCostCapabilities(definition *model.PipelineDefinition, available []string) ([]string, error) {
	if definition.Cost == nil || !definition.Cost.Enabled {
		return nil, nil
	}
	pricing := "network.pricing"
	if slices.Contains(available, "pricing.local") && !slices.Contains(definition.RequiredCapabilities, "network.pricing") {
		pricing = "pricing.local"
	}
	if !slices.Contains(available, "cost.infracost.v1") || !slices.Contains(available, pricing) {
		return nil, model.NewErrConflict("cost estimation requires qualified cost estimation and reachable site-local pricing access")
	}
	return []string{"cost.infracost.v1", pricing}, nil
}

func (s *Server) bindRequestedPipelineCostPolicy(ctx context.Context, definition *model.PipelineDefinition) error {
	if definition == nil || definition.Cost == nil || !definition.Cost.Enabled {
		return nil
	}
	policy, err := model.ReadCostPolicy(ctx, s.Database)
	if err != nil {
		return err
	}
	if !policy.Enabled {
		return model.NewErrConflict(model.CostEstimationDisabledMessage)
	}
	definition.Cost.PolicyRevision = policy.Revision
	return nil
}
