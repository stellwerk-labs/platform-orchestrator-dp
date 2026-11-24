package api

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestPipelineSupersededPlanExplainsRunningReplacement(t *testing.T) {
	base := uuid.New()
	status := &pipelinePlanStatus{BaseDeploymentID: &base}
	dep := &model.DeploymentSummary{Id: uuid.New(), Status: model.DeploymentStatusSucceeded}
	run := &model.DeploymentPipeline{Progress: model.PipelineProgress{Subject: "reviewed-source"}}
	result := completePipelinePlanStatus(status, dep, run, uuid.New(), true)
	require.Equal(t, pipelinePlanSuperseded, result.State)
	require.False(t, result.CanReplan)
	require.Contains(t, result.Reason, "still running")
	require.Contains(t, result.Reason, "after it finishes")
}
