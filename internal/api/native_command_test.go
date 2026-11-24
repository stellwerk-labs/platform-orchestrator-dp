package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestNativeCommandBindsFrozenTargetWithoutLegacyCapability(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	target := &cp.NativeExecutionTargetReference{
		RunnerId:                 "site",
		RunnerUuid:               uuid.New(),
		RuntimeVersion:           cp.NativeTargetRuntimeV2,
		PipelineVersion:          cp.NativeTargetPipelineV2,
		ToolsDigest:              strings.Repeat("a", 64),
		StateBackendType:         cp.StateStorageTypeKubernetes,
		StateConfigurationDigest: strings.Repeat("b", 64),
	}
	dep := &model.DeploymentSummary{NativeTarget: target, Id: uuid.New(), OrgId: "org", RunnerId: "site", DeploymentEnvUuid: uuid.New()}
	// No legacy adapter, token salt, digest or metadata configuration is needed by this signer.
	dispatcher := PipelineDispatcher{Server: &Server{}, Key: key, ToolsDigest: "must-not-enter-native", MetadataKey: "must-not-enter-native"}
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	raw, err := dispatcher.signCommand(dep, *definition, []byte("frozen fixture bundle"), json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.NoError(t, err)
	payload, err := model.FrozenPipelinePayload(raw, dep)
	require.NoError(t, err)
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(payload, &command))
	require.Equal(t, target, command.NativeTarget)
	require.NotNil(t, command.ExecutionLimits)
	require.True(t, command.ExecutionLimits.Valid())
	require.Equal(t, "stellwerk.pipeline/v2", command.Version)
	require.Equal(t, target.RunnerUuid.String(), command.RunnerUUID)
	require.Equal(t, command.RunnerUUID, command.Request.RunnerUUID)
	require.Equal(t, "opentofu", command.Request.Tool)
	require.Empty(t, command.Request.MetadataKey)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &fields))
	require.NoError(t, json.Unmarshal(fields["request"], &fields))
	require.NotContains(t, fields, "deployment_token")
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(raw, &envelope))
	route, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	require.NoError(t, err)
	require.Equal(t, route, envelope.RunnerID)
	require.Equal(t, hmessaging.ProtocolVersionV1, envelope.ProtocolVersion)
	require.Equal(t, "execute-rollout-v1", envelope.Type)
	for _, change := range []func(*cp.NativeExecutionTargetReference){
		func(v *cp.NativeExecutionTargetReference) { v.RunnerId = "other" }, func(v *cp.NativeExecutionTargetReference) { v.RunnerUuid = uuid.New() }, func(v *cp.NativeExecutionTargetReference) { v.RuntimeVersion = "other" }, func(v *cp.NativeExecutionTargetReference) { v.PipelineVersion = "other" }, func(v *cp.NativeExecutionTargetReference) { v.ToolsDigest = strings.Repeat("c", 64) }, func(v *cp.NativeExecutionTargetReference) { v.StateBackendType = cp.StateStorageTypeS3 }, func(v *cp.NativeExecutionTargetReference) { v.StateConfigurationDigest = strings.Repeat("c", 64) },
	} {
		changed := *target
		change(&changed)
		owner := *dep
		owner.NativeTarget = &changed
		_, err = model.FrozenPipelinePayload(raw, &owner)
		require.Error(t, err)
	}
	for _, bad := range []string{
		strings.Replace(string(payload), `"execution_limits":{"plan_seconds":1800,"apply_seconds":7200,"task_seconds":1800},`, ``, 1),
		strings.Replace(string(payload), `"execution_limits":`, `"EXECUTION_LIMITS":`, 1),
		strings.Replace(string(payload), `"execution_limits":{"plan_seconds":1800,"apply_seconds":7200,"task_seconds":1800}`, `"execution_limits":null`, 1),
		strings.TrimSuffix(string(payload), "}") + `,"execution_limits":{"plan_seconds":1,"apply_seconds":1,"task_seconds":1}}`,
		strings.Replace(string(payload), `"request":{`, `"request":{"deployment_token":"",`, 1),
		strings.Replace(string(payload), `"request":{`, `"request":{"DEPLOYMENT_TOKEN":null,`, 1),
		strings.Replace(string(payload), `"request":{`, `"request":{"deployment_to\u006ben":false,`, 1),
		strings.Replace(string(payload), `"request":`, `"request":{"deployment_token":null},"request":`, 1),
		strings.Replace(string(payload), `"request":`, `"Request":`, 1),
		strings.Replace(string(payload), `"request":`, `"reque\u0073t":{"deployment_token":null},"request":`, 1),
	} {
		var signed map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
		signed["payload"] = json.RawMessage(bad)
		envelope.Payload, err = json.Marshal(signed)
		require.NoError(t, err)
		malformed, err := json.Marshal(envelope)
		require.NoError(t, err)
		_, err = model.FrozenPipelinePayload(malformed, dep)
		require.Error(t, err)
	}
	// Core does not accept the standalone Engine's local backend contract.
	local := *target
	local.StateBackendType = "local"
	owner := *dep
	owner.NativeTarget = &local
	_, err = dispatcher.signCommand(&owner, *definition, []byte("fixture"), json.RawMessage(`{}`), "local", nil, nil, "")
	require.Error(t, err)
	dispatcher.NativeLimits = &model.PipelineExecutionLimits{PlanSeconds: 1, ApplySeconds: 1, TaskSeconds: 14401}
	_, err = dispatcher.signCommand(dep, *definition, []byte("fixture"), json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.ErrorContains(t, err, "execution limits")
}

func TestLegacyPipelinePayloadMatchesOriginalWire(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: &Server{RunnerTokenSalt: "legacy-salt"}, Key: key, ToolsDigest: strings.Repeat("a", 64), MetadataKey: "metadata"}
	dep := &model.DeploymentSummary{Id: uuid.MustParse("11111111-1111-4111-8111-111111111111"), OrgId: "org", RunnerId: "site", DeploymentEnvUuid: uuid.MustParse("22222222-2222-4222-8222-222222222222"), Mode: model.DeploymentModeDeploy}
	definition, err := model.ResolvePipeline(nil, dep.Mode)
	require.NoError(t, err)
	raw, err := dispatcher.signCommand(dep, *definition, []byte("closure"), json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.NoError(t, err)
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(raw, &envelope))
	payload, err := model.FrozenPipelinePayload(raw, dep)
	require.NoError(t, err)
	golden, err := os.ReadFile("testdata/pipeline-command-v1-payload.golden")
	require.NoError(t, err)
	expected := strings.TrimSpace(string(golden))
	expected = strings.ReplaceAll(expected, "$ISSUED_AT", envelope.CreatedAt.Format(time.RFC3339Nano))
	expected = strings.ReplaceAll(expected, "$EXPIRES_AT", envelope.ExpiresAt.Format(time.RFC3339Nano))
	require.Equal(t, expected, string(payload), "valid v1 field order and values must remain byte-compatible")
	require.Equal(t, "site", envelope.RunnerID)
	require.Equal(t, "pipeline:"+dep.Id.String(), envelope.CommandID)
}

func TestNativeObservationProducerMatchesFrozenAuthority(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	target := &cp.NativeExecutionTargetReference{
		RunnerId:                 "site",
		RunnerUuid:               uuid.New(),
		RuntimeVersion:           cp.NativeTargetRuntimeV2,
		PipelineVersion:          cp.NativeTargetPipelineV2,
		ToolsDigest:              strings.Repeat("a", 64),
		StateBackendType:         cp.StateStorageTypeKubernetes,
		StateConfigurationDigest: strings.Repeat("b", 64),
	}
	dep := &model.DeploymentSummary{NativeTarget: target, Id: uuid.New(), OrgId: "org", RunnerId: "site", DeploymentEnvUuid: uuid.New(), Mode: model.DeploymentModeObservation}
	dispatcher := PipelineDispatcher{Server: &Server{}, Key: key}
	definition := model.PipelineDefinition{Strategy: nativeObservationStrategy, Steps: []model.PipelineStep{}, RequiredCapabilities: []string{"environment-observation/v1"}}
	raw, err := dispatcher.signCommand(dep, definition, []byte("immutable observation archive"), json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.NoError(t, err)
	payload, err := model.FrozenPipelinePayload(raw, dep)
	require.NoError(t, err)
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(payload, &command))
	require.Equal(t, "observation", command.Request.Mode)
	require.Empty(t, command.Request.EncryptedOutputsRecipient)
	require.Empty(t, command.Request.MetadataKey)
	require.Empty(t, command.Steps)
	require.Equal(t, target, command.NativeTarget)
	require.NotNil(t, command.ExecutionLimits)
	require.True(t, command.ExecutionLimits.Valid())
}
