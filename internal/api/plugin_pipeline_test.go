package api

import (
	"github.com/google/uuid"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPluginPipelineIdentityPreservesPublishedAndBuiltinReferences(t *testing.T) {
	versionID, pipelineID := uuid.New(), uuid.New()
	identity, err := pipelineLifecycleIdentity(model.PipelineDefinition{ID: versionID.String(), PipelineID: pipelineID.String(), Version: 2, Digest: "exact-published-digest"})
	require.NoError(t, err)
	require.Equal(t, versionID, *identity.VersionId)
	require.Equal(t, pipelineID, *identity.PipelineId)
	require.Equal(t, versionID, *identity.Reference.VersionId)
	require.Nil(t, identity.Reference.Builtin)
	identity, err = pipelineLifecycleIdentity(model.PipelineDefinition{ID: model.PipelineDefault, Version: 1})
	require.NoError(t, err)
	require.Nil(t, identity.PipelineId)
	require.Nil(t, identity.VersionId)
	require.Nil(t, identity.Reference.VersionId)
	require.Equal(t, "default", string(*identity.Reference.Builtin))
	require.Equal(t, p.BuiltinReference().Digest, identity.Reference.Digest)
}
func TestExternalPipelineSelectionCannotSubstituteCurrentDefault(t *testing.T) {
	published := publishedRecipeFixture(t)
	choices := &p.PipelineChoices{Policy: p.PipelinePolicy{Enabled: true, DefaultVersionId: &published.Id}, Versions: []p.PublishedPipelineVersion{published}}
	ref := p.PipelineReference{VersionId: &published.Id, Digest: published.Digest}
	definition, err := externalPipelineDefinition(choices, ref, model.DeploymentModeRollback)
	require.NoError(t, err)
	require.Equal(t, published.Id.String(), definition.ID)
	require.Equal(t, "reviewed", definition.Strategy)
	require.Len(t, definition.Steps, 3)
	ref.Digest = "changed"
	_, err = externalPipelineDefinition(choices, ref, model.DeploymentModeDeploy)
	require.ErrorContains(t, err, "unavailable")
	_, err = externalPipelineDefinition(choices, p.BuiltinReference(), model.DeploymentModeDeploy)
	require.ErrorContains(t, err, "requires")
	choices.Policy.Enabled = false
	definition, err = externalPipelineDefinition(choices, p.BuiltinReference(), model.DeploymentModeRollback)
	require.NoError(t, err)
	require.Equal(t, "apply", definition.Strategy)
	require.Empty(t, definition.Steps)
}

func TestPipelineLifecycleAttentionUsesCanonicalApprovalPhase(t *testing.T) {
	reason := pipelineAttentionReason("awaiting_approval")
	require.NotNil(t, reason)
	require.Equal(t, "awaiting_approval", *reason)
	require.Nil(t, pipelineAttentionReason("planning"))
}

func TestProtectedNormalRollbackCannotSilentlyUseForwardDefault(t *testing.T) {
	version := publishedRecipeFixture(t)
	choices := &p.PipelineChoices{Policy: p.PipelinePolicy{Enabled: true, DefaultVersionId: &version.Id}, Versions: []p.PublishedPipelineVersion{version}}
	_, err := selectProjectPipeline(choices, nil, model.DeploymentModeRollback)
	require.ErrorContains(t, err, "explicit published pipeline")
	selected, err := selectProjectPipeline(choices, &model.PipelineSelection{ID: version.Id.String(), Version: version.Version}, model.DeploymentModeRollback)
	require.NoError(t, err)
	require.Equal(t, version.Id.String(), selected.ID)
	choices.Policy.Enabled = false
	selected, err = selectProjectPipeline(choices, nil, model.DeploymentModeRollback)
	require.NoError(t, err)
	require.Nil(t, selected, "unconfigured legacy rollback retains direct Apply behavior")
}

func TestNormalPipelineUsesOnlyItsDirectionalDefault(t *testing.T) {
	forward, rollback, environment := publishedRecipeFixture(t), publishedRecipeFixture(t), publishedRecipeFixture(t)
	for _, trial := range []struct {
		name      string
		mode      model.DeploymentMode
		effective p.PublishedPipelineVersion
		requested *model.PipelineSelection
		want      uuid.UUID
	}{
		{name: "project rollback", mode: model.DeploymentModeRollback, effective: rollback, want: rollback.Id},
		{name: "environment rollback overrides project", mode: model.DeploymentModeRollback, effective: environment, want: environment.Id},
		{name: "explicit selection overrides rollback default", mode: model.DeploymentModeRollback, effective: rollback, requested: &model.PipelineSelection{ID: forward.Id.String()}, want: forward.Id},
		{name: "forward ignores rollback default", mode: model.DeploymentModeDeploy, effective: rollback, want: forward.Id},
	} {
		t.Run(trial.name, func(t *testing.T) {
			choices := &p.PipelineChoices{
				Policy: p.PipelinePolicy{Enabled: true, DefaultVersionId: &forward.Id,
					RollbackDefaults: &p.PipelineRollbackDefaults{DefaultVersionId: &rollback.Id}},
				Versions:                 []p.PublishedPipelineVersion{forward, rollback, environment},
				EffectiveDefault:         &p.PipelineReference{VersionId: &forward.Id, Digest: forward.Digest},
				EffectiveRollbackDefault: &p.PipelineReference{VersionId: &trial.effective.Id, Digest: trial.effective.Digest},
			}
			selected, err := selectProjectPipeline(choices, trial.requested, trial.mode)
			require.NoError(t, err)
			require.Equal(t, trial.want.String(), selected.ID)
		})
	}
}

func TestNormalRollbackRefusesUnavailableEffectiveDefault(t *testing.T) {
	forward, rollback := publishedRecipeFixture(t), publishedRecipeFixture(t)
	unavailable := uuid.New()
	for _, reference := range []*p.PipelineReference{nil, {}, {VersionId: &unavailable}} {
		choices := &p.PipelineChoices{
			Policy: p.PipelinePolicy{Enabled: true, DefaultVersionId: &forward.Id,
				RollbackDefaults: &p.PipelineRollbackDefaults{DefaultVersionId: &rollback.Id}},
			Versions:                 []p.PublishedPipelineVersion{forward, rollback},
			EffectiveDefault:         &p.PipelineReference{VersionId: &forward.Id, Digest: forward.Digest},
			EffectiveRollbackDefault: reference,
		}
		_, err := selectProjectPipeline(choices, nil, model.DeploymentModeRollback)
		require.Error(t, err, "never substitute raw project or forward defaults for unavailable Core resolution")
	}
}
