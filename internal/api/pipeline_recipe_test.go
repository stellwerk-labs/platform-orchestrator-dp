package api

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func publishedRecipeFixture(t *testing.T) p.PublishedPipelineVersion {
	t.Helper()
	recipe := p.PipelineRecipe{Strategy: p.Reviewed, Steps: []p.PipelineRecipeStep{
		{Id: "validate", ToolId: p.ValidateManifest, Phase: p.PrePlan, Parameters: map[string]any{}},
		{Id: "policy", ToolId: p.ResourcePolicy, Phase: p.PlanCheck, Parameters: map[string]any{}},
		{Id: "bootstrap", ToolId: p.BootstrapDatabase, Phase: p.PostApply, Parameters: map[string]any{}},
	}}
	digest, err := p.RecipeDigest("Review and bootstrap", recipe)
	require.NoError(t, err)
	return p.PublishedPipelineVersion{Id: uuid.New(), PipelineId: uuid.New(), ProjectId: "project", Version: 3, Name: "Review and bootstrap", Definition: recipe, Digest: digest}
}
func recipeRunner(t *testing.T, capabilities ...string) cp.Runner {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"type": "kubernetes-agent", "supported_protocols": append([]string{model.PipelineProtocol}, capabilities...)})
	require.NoError(t, err)
	var config cp.RunnerConfiguration
	require.NoError(t, json.Unmarshal(raw, &config))
	return cp.Runner{RunnerConfiguration: config}
}
func TestPublishedRecipePreservesManualPlanAndSavedApplyAuthority(t *testing.T) {
	version := publishedRecipeFixture(t)
	plan, err := publishedPipeline(version, model.DeploymentModeDeployPlan)
	require.NoError(t, err)
	require.Equal(t, "plan", plan.Strategy)
	require.Equal(t, "reviewed", plan.IntendedStrategy)
	require.Len(t, plan.Steps, 3)
	require.NoError(t, validatePinnedRecipe(plan))
	child := *plan
	child.Strategy = "apply_saved"
	require.NoError(t, validatePinnedRecipe(&child))
	child.Steps = child.Steps[:2]
	require.ErrorContains(t, validatePinnedRecipe(&child), "changed", "post tasks cannot disappear during saved-Plan promotion")
	rollback, err := publishedPipeline(version, model.DeploymentModeRollback)
	require.NoError(t, err)
	require.Equal(t, "reviewed", rollback.Strategy)
	require.Equal(t, version.Digest, rollback.Digest)
	require.Len(t, rollback.Steps, 3)
}
func TestPublishedRecipeRequiresActualRunnerToolAndCostCapabilities(t *testing.T) {
	version := publishedRecipeFixture(t)
	definition, err := publishedPipeline(version, model.DeploymentModeDeploy)
	require.NoError(t, err)
	_, err = pipelineForRunner(recipeRunner(t), model.DeploymentModeDeploy, definition)
	require.ErrorContains(t, err, "immutable project pipeline")
	_, err = pipelineForRunner(recipeRunner(t, "pipeline.recipe.v1", "local.exec", "artifacts.read"), model.DeploymentModeDeploy, definition)
	require.ErrorContains(t, err, "database.local")
	capabilities := []string{"pipeline.recipe.v1", "local.exec", "artifacts.read", "database.local"}
	_, err = pipelineForRunner(recipeRunner(t, capabilities...), model.DeploymentModeDeploy, definition)
	require.NoError(t, err)
	version.Definition.Cost = &struct {
		Enabled bool `json:"enabled"`
	}{true}
	version.Definition.Approval = &struct {
		Mode p.PipelineRecipeApprovalMode `json:"mode"`
	}{p.CostNonIncreasing}
	version.Digest, err = p.RecipeDigest(version.Name, version.Definition)
	require.NoError(t, err)
	definition, err = publishedPipeline(version, model.DeploymentModeDeploy)
	require.NoError(t, err)
	_, err = pipelineForRunner(recipeRunner(t, append(capabilities, "cost.infracost.v1")...), model.DeploymentModeDeploy, definition)
	require.ErrorContains(t, err, "pricing")
	_, err = pipelineForRunner(recipeRunner(t, append(capabilities, "cost.infracost.v1", "pricing.local")...), model.DeploymentModeDeploy, definition)
	require.NoError(t, err)
	require.Equal(t, "3", definition.Approval.PolicyVersion)
}
