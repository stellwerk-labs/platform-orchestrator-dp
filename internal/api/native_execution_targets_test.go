package api

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func nativeTargetFixture() *cp.NativeExecutionTargetReference {
	return &cp.NativeExecutionTargetReference{
		RunnerId:                 "native_pool",
		RunnerUuid:               uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		RuntimeVersion:           cp.NativeTargetRuntimeV2,
		PipelineVersion:          cp.NativeTargetPipelineV2,
		ToolsDigest:              strings.Repeat("a", 64),
		StateBackendType:         cp.StateStorageType("s3"),
		StateConfigurationDigest: strings.Repeat("b", 64),
	}
}

func TestCapturedPlanFingerprintBindsNativeTargetAndPreservesLegacy(t *testing.T) {
	capture := DeploymentPlanCapture{BaseDeploymentID: uuid.MustParse("11111111-1111-4111-8111-111111111111"), Manifest: []byte(`{"workloads":{}}`), Graph: []byte(`{}`), Diff: []byte(`{}`)}
	require.Equal(t, "sha256:ab4f7c20c925d9398b212f6123441dccdd6396b154fa38ef9d8bc3d30c8618e6", deploymentPlanFingerprint(capture))
	capture.NativeTarget = nativeTargetFixture()
	original := deploymentPlanFingerprint(capture)
	for _, change := range []func(*cp.NativeExecutionTargetReference){
		func(v *cp.NativeExecutionTargetReference) { v.RunnerId = "another" }, func(v *cp.NativeExecutionTargetReference) { v.RunnerUuid = uuid.New() },
		func(v *cp.NativeExecutionTargetReference) { v.RuntimeVersion = "other" }, func(v *cp.NativeExecutionTargetReference) { v.PipelineVersion = "other" },
		func(v *cp.NativeExecutionTargetReference) { v.ToolsDigest = "different" }, func(v *cp.NativeExecutionTargetReference) { v.StateBackendType = "gcs" }, func(v *cp.NativeExecutionTargetReference) { v.StateConfigurationDigest = "different" },
	} {
		changed := *capture.NativeTarget
		change(&changed)
		capture.NativeTarget = &changed
		require.NotEqual(t, original, deploymentPlanFingerprint(capture))
		capture.NativeTarget = nativeTargetFixture()
	}
	saved := pipelinePlanCapture{NativeTarget: capture.NativeTarget}
	raw, err := json.Marshal(saved)
	require.NoError(t, err)
	var restored pipelinePlanCapture
	require.NoError(t, json.Unmarshal(raw, &restored))
	require.Equal(t, saved.NativeTarget, restored.NativeTarget)
}

func TestNativeTargetRefusesDirectV1DispatchBeforeServerOrPublisherEffects(t *testing.T) {
	dispatcher := &PipelineDispatcher{}
	dep := &model.DeploymentSummary{NativeTarget: nativeTargetFixture(), Mode: model.DeploymentModeObservation}
	require.ErrorContains(t, dispatcher.Dispatch(context.Background(), dep), "requires v2 dispatch")
	require.ErrorContains(t, dispatcher.DispatchObservation(context.Background(), dep), "Native observation target is invalid")
}

func TestNativeAgentCannotFallThroughLegacyPipelineSelection(t *testing.T) {
	var runner cp.Runner
	require.NoError(t, json.Unmarshal([]byte(`{"runner_configuration":{"type":"native-agent","supported_protocols":["stellwerk.runner.rollout/v2","stellwerk.pipeline/v2"]}}`), &runner))
	for _, mode := range []model.DeploymentMode{model.DeploymentModeDeploy, model.DeploymentModeDeployPlan, model.DeploymentModeRollback, model.DeploymentModeDestroy} {
		_, err := pipelineForRunner(runner, mode, nil)
		require.ErrorContains(t, err, "native Runner registration identity is unavailable")
	}
}

func TestNativeDispatchMalformedTargetRefusesBeforeDependencies(t *testing.T) {
	for _, change := range []func(*cp.NativeExecutionTargetReference){
		func(v *cp.NativeExecutionTargetReference) { v.RunnerId = "other" },
		func(v *cp.NativeExecutionTargetReference) { v.RunnerUuid = uuid.Nil },
		func(v *cp.NativeExecutionTargetReference) { v.RuntimeVersion = "old" },
		func(v *cp.NativeExecutionTargetReference) { v.PipelineVersion = "old" },
		func(v *cp.NativeExecutionTargetReference) { v.ToolsDigest = "bad" },
		func(v *cp.NativeExecutionTargetReference) { v.StateBackendType = "unknown" },
		func(v *cp.NativeExecutionTargetReference) { v.StateConfigurationDigest = "bad" },
	} {
		target := nativeTargetFixture()
		change(target)
		dispatcher := &PipelineDispatcher{}
		dep := &model.DeploymentSummary{RunnerId: "native_pool", NativeTarget: target, HasPipeline: true, Mode: model.DeploymentModeDeploy}
		require.ErrorContains(t, dispatcher.Dispatch(t.Context(), dep), "requires v2 dispatch")
	}
}
