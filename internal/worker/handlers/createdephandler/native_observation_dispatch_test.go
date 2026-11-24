package createdephandler

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFrozenNativeObservationNeverReinterpretsCurrentRunner(t *testing.T) {
	dep := &model.DeploymentSummary{Mode: model.DeploymentModeObservation, HasNativeObservation: true}
	called := 0
	handler := CreateDepHandler{ObservationDispatch: func(context.Context, *model.DeploymentSummary) error { called++; return nil }}
	require.NoError(t, handler.handleInner(t.Context(), zap.NewNop(), dep))
	require.Equal(t, 1, called, "frozen observation routing cannot consult the mutable Runner config or legacy executor")
	handler.ObservationDispatch = nil
	_, retry := hmessaging.AsRetryError(handler.handleInner(t.Context(), zap.NewNop(), dep))
	require.True(t, retry, "an unavailable native observer cannot fall back to a different executor")
}

func TestNativeObservationRoutesFirstAndRetainedDispatchWithoutLegacyFallback(t *testing.T) {
	for _, retained := range []bool{false, true} {
		dep := &model.DeploymentSummary{Mode: model.DeploymentModeObservation, HasNativeObservation: retained,
			RunnerId: "site", NativeTarget: &cp.NativeExecutionTargetReference{
				RunnerId: "site", RunnerUuid: uuid.New(), RuntimeVersion: cp.NativeTargetRuntimeV2,
				PipelineVersion: cp.NativeTargetPipelineV2, ToolsDigest: strings.Repeat("a", 64),
				StateBackendType: cp.StateStorageTypeKubernetes, StateConfigurationDigest: strings.Repeat("b", 64),
			}}
		called := 0
		handler := CreateDepHandler{ObservationDispatch: func(_ context.Context, actual *model.DeploymentSummary) error {
			require.Same(t, dep, actual)
			called++
			return nil
		}}
		require.NoError(t, handler.handleInner(t.Context(), zap.NewNop(), dep))
		require.Equal(t, 1, called)
		handler.ObservationDispatch = nil
		_, retry := hmessaging.AsRetryError(handler.handleInner(t.Context(), zap.NewNop(), dep))
		require.True(t, retry)
		dep.HasPipeline = true
		handler.PipelineDispatch = func(context.Context, *model.DeploymentSummary) error {
			t.Fatal("an observation cannot become a deployment pipeline")
			return nil
		}
		require.Error(t, handler.handleInner(t.Context(), zap.NewNop(), dep))
	}
}
