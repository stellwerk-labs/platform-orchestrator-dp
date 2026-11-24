package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestFrozenNativeObservationRejectsCustomerPipelineAuthority(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: &Server{}, Key: key}
	target := nativeTargetFixture()
	dep := &model.DeploymentSummary{Id: uuid.New(), OrgId: "org", RunnerId: target.RunnerId,
		DeploymentEnvUuid: uuid.New(), Mode: model.DeploymentModeObservation, NativeTarget: target}
	definition := model.PipelineDefinition{Strategy: nativeObservationStrategy, RequiredCapabilities: []string{"environment-observation/v1"}}
	raw, err := dispatcher.signCommand(dep, definition, []byte("retained baseline"), json.RawMessage(`{}`), string(target.StateBackendType), nil, nil, "")
	require.NoError(t, err)
	require.NoError(t, validateFrozenObservationCommand(raw, dep))
	for _, trial := range []struct {
		name   string
		change func(*pipelineCommand)
	}{
		{"strategy", func(c *pipelineCommand) { c.Strategy = "apply" }},
		{"capability", func(c *pipelineCommand) { c.RequiredCapabilities = nil }},
		{"step", func(c *pipelineCommand) { c.Steps = []pipelineCommandStep{{ID: "customer-task"}} }},
		{"recipe", func(c *pipelineCommand) { c.Recipe = &pipelineRecipeReference{ID: "customer-pipeline"} }},
		{"cost", func(c *pipelineCommand) { c.Cost = &model.PipelineCost{} }},
		{"approval", func(c *pipelineCommand) { c.Approval.Mode = expectedLocalApprovalMode }},
		{"plan base", func(c *pipelineCommand) { c.PlanBase = &pipelinePlanBase{BaseDeploymentID: uuid.NewString()} }},
		{"source plan", func(c *pipelineCommand) { c.SourcePlan = &pipelineSourcePlan{SourcePlanID: uuid.NewString()} }},
		{"metadata", func(c *pipelineCommand) { c.Request.MetadataKey = "private" }},
		{"output recipient", func(c *pipelineCommand) { c.Request.EncryptedOutputsRecipient = "private" }},
		{"mode", func(c *pipelineCommand) { c.Request.Mode = "deploy" }},
	} {
		t.Run(trial.name, func(t *testing.T) {
			changed := resignObservationFixture(t, raw, key, trial.change)
			require.Error(t, validateFrozenObservationCommand(changed, dep))
		})
	}
}

func resignObservationFixture(t *testing.T, raw []byte, key ed25519.PrivateKey, change func(*pipelineCommand)) []byte {
	t.Helper()
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(raw, &envelope))
	var signed struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(signed.Payload, &command))
	change(&command)
	payload, err := json.Marshal(command)
	require.NoError(t, err)
	signed.Payload, signed.Signature = payload, base64.StdEncoding.EncodeToString(ed25519.Sign(key, payload))
	envelope.Payload, err = json.Marshal(signed)
	require.NoError(t, err)
	result, err := json.Marshal(envelope)
	require.NoError(t, err)
	return result
}
