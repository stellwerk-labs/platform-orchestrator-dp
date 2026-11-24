//go:build integration

package api

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stretchr/testify/require"
)

func TestPipelineCatalogueCostEligibilityUsesExistingActivation(t *testing.T) {
	f := nativePlanFixture(t)
	definitions := []model.PipelineDefinition{{ID: "cost", Cost: &model.PipelineCost{Enabled: true}}}
	enabled, err := f.server.catalogueCostEnabled(f.ctx, definitions)
	require.NoError(t, err)
	require.False(t, enabled)
	_, err = model.UpdateCostPolicy(f.ctx, f.store, uuid.New(), 1, true, ref.Ref("vault:v1:owned-test-ciphertext"))
	require.NoError(t, err)
	enabled, err = f.server.catalogueCostEnabled(f.ctx, definitions)
	require.NoError(t, err)
	require.True(t, enabled)
	definition := &model.PipelineDefinition{Cost: &model.PipelineCost{Enabled: true}}
	require.NoError(t, f.server.bindRequestedPipelineCostPolicy(f.ctx, definition))
	require.EqualValues(t, 2, definition.Cost.PolicyRevision)
}
