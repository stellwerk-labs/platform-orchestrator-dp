package api

import (
	"encoding/json"
	"regexp"
	"slices"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const nativeExecutionUnavailableMessage = "native execution requires qualified v2 dispatch and access isolation"
const pipelinePostApplyPhase = "post_apply"

var nativeRunnerID = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,127}$`)
var nativeToolsDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

// nativeExecutionTarget qualifies metadata only. It does not enable Native dispatch.
func nativeExecutionTarget(org, runnerID string, runner cp.Runner, mode model.DeploymentMode, definition *model.PipelineDefinition) (*cp.NativeExecutionTargetReference, error) {
	kind, _ := runner.RunnerConfiguration.Discriminator()
	if kind != string(cp.RunnerTypeNativeAgent) {
		return nil, nil
	}
	switch mode {
	case model.DeploymentModeDeploy, model.DeploymentModeDeployPlan, model.DeploymentModeRollback, model.DeploymentModeRollbackPlan, model.DeploymentModeDestroy, model.DeploymentModeObservation:
	default:
		return nil, model.NewErrConflict("native Runner execution mode is unsupported")
	}
	if runner.OrgId != org || runner.Id != runnerID || !nativeRunnerID.MatchString(runner.Id) || runner.Uuid == nil || *runner.Uuid == uuid.Nil {
		return nil, model.NewErrConflict("native Runner registration identity is unavailable")
	}
	if err := validateNativeJobConfiguration(runner.RunnerConfiguration); err != nil {
		return nil, err
	}
	config, err := runner.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
	if err != nil || config.MaxParallelJobs <= 0 || config.NativeExecution == nil || config.SupportedProtocols == nil || config.Capabilities == nil {
		return nil, model.NewErrConflict("native Runner execution is disabled or incomplete")
	}
	catalogue := config.NativeExecution
	if catalogue.ContractVersion != cp.StellwerkRunnerCataloguev1 || catalogue.RuntimeVersion != cp.StellwerkRunnerRolloutv2 || !nativeToolsDigest.MatchString(catalogue.ToolsDigest) ||
		!slices.Contains(*config.SupportedProtocols, string(cp.NativeTargetPipelineV2)) || !slices.Contains(*config.SupportedProtocols, "stellwerk.pipeline.plan-apply/v2") {
		return nil, model.NewErrConflict("native Runner requires qualified v2 execution protocols")
	}
	raw, err := json.Marshal(runner.StateStorageConfiguration)
	if err != nil {
		return nil, err
	}
	backend, digest, err := cp.StateStorageConfigurationDigest(raw)
	if err != nil {
		return nil, model.NewErrConflict("native Runner state configuration is invalid")
	}
	target := &cp.NativeExecutionTargetReference{RunnerId: runner.Id, RunnerUuid: *runner.Uuid, RuntimeVersion: cp.NativeTargetRuntimeV2, PipelineVersion: cp.NativeTargetPipelineV2, ToolsDigest: catalogue.ToolsDigest, StateBackendType: backend, StateConfigurationDigest: digest}
	if mode == model.DeploymentModeObservation {
		if definition != nil || !slices.Contains(*config.SupportedProtocols, "environment-observation/v1") ||
			!slices.Contains(*config.Capabilities, "environment-observation/v1") {
			return nil, model.NewErrConflict("native Runner requires the dedicated read-only observer without a customer recipe")
		}
		return target, nil
	}
	if definition == nil {
		definition, err = model.ResolvePipeline(nil, mode)
		if err != nil {
			return nil, err
		}
	}
	qualified := *definition
	available := append(slices.Clone(*config.SupportedProtocols), (*config.Capabilities)...)
	if err = qualifyRecipeCapabilities(&qualified, available); err != nil {
		return nil, err
	}
	if err = qualifyNativeTools(&qualified, catalogue.Tools, available); err != nil {
		return nil, err
	}
	return target, nil
}

func qualifyNativeTools(definition *model.PipelineDefinition, tools []cp.RunnerExecutionTool, available []string) error {
	for _, step := range definition.Steps {
		index := slices.IndexFunc(tools, func(tool cp.RunnerExecutionTool) bool { return tool.Id == step.ToolID })
		if index < 0 {
			return model.NewErrConflict("selected native task is not installed")
		}
		tool := tools[index]
		retry := cp.RunnerExecutionToolRetryReadOnly
		if step.Phase == pipelinePostApplyPhase {
			retry = cp.RunnerExecutionToolRetryReceipt
		}
		if string(tool.Phase) != step.Phase || tool.Retry != retry {
			return model.NewErrConflict("selected native task phase or retry contract changed")
		}
		for _, capability := range tool.RequiredCapabilities {
			if !slices.Contains(available, capability) {
				return model.NewErrConflict("selected native task requires unavailable capability")
			}
		}
	}
	return nil
}

// Only Core-created Native-first realms may start a fresh Native execution.
func requireNativeFirstEnvironment(env cp.Environment) error {
	if env.NativeFirstEligible == nil || !*env.NativeFirstEligible {
		return model.NewErrConflict("Environment is not eligible for Native-first execution")
	}
	return nil
}
