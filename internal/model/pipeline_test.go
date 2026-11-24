package model

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPipelineStrategiesPreserveExplicitPlan(t *testing.T) {
	for _, mode := range []DeploymentMode{DeploymentModeDeploy, DeploymentModeRollback} {
		definition, err := ResolvePipeline(nil, mode)
		require.NoError(t, err)
		require.Equal(t, "apply", definition.Strategy)
		require.Empty(t, definition.Steps)
	}
	for _, mode := range []DeploymentMode{DeploymentModeDeployPlan, DeploymentModeRollbackPlan} {
		definition, err := ResolvePipeline(nil, mode)
		require.NoError(t, err)
		require.Equal(t, "plan", definition.Strategy)
		_, err = ResolvePipeline(&PipelineSelection{ID: PipelineReviewed, Version: 1}, mode)
		require.ErrorContains(t, err, "manual Plan")
	}
	definition, err := ResolvePipeline(nil, DeploymentModeDestroy)
	require.NoError(t, err)
	require.Equal(t, "destroy", definition.Strategy)
	_, err = ResolvePipeline(nil, DeploymentModeObservation)
	require.ErrorContains(t, err, "observations")
	_, err = ResolvePipeline(&PipelineSelection{ID: PipelineDefault, Version: 2}, DeploymentModeDeploy)
	require.ErrorContains(t, err, "unknown pipeline version")
}
