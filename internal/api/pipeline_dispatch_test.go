package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

const expectedLocalApprovalMode = "local"

func TestPipelineCommandBindsCanonicalBundleAndRecipe(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: &Server{RunnerTokenSalt: "test-only-salt"}, Key: private, ToolsDigest: "pinned-catalog", MetadataKey: "metadata"}
	dep := &model.DeploymentSummary{Id: uuid.New(), OrgId: "org", RunnerId: "site", DeploymentEnvUuid: uuid.New()}
	archive := []byte("compiled immutable archive")
	definition, err := model.ResolvePipeline(&model.PipelineSelection{ID: model.PipelineReviewed, Version: 1}, model.DeploymentModeDeploy)
	require.NoError(t, err)
	raw, err := dispatcher.signCommand(dep, *definition, archive, json.RawMessage(`{"workloads":{},"environment_id":"canonical"}`), "kubernetes", nil, nil, "")
	require.NoError(t, err)
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(raw, &envelope))
	require.Equal(t, dep.Id.String(), envelope.DeploymentID)
	require.Equal(t, "execute-rollout-v1", envelope.Type)
	var signed struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	require.NoError(t, err)
	require.True(t, ed25519.Verify(public, signed.Payload, signature))
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(signed.Payload, &command))
	require.Equal(t, model.PipelineProtocol, command.Version)
	require.Equal(t, "reviewed", command.Strategy)
	require.Equal(t, expectedLocalApprovalMode, command.Approval.Mode)
	require.Equal(t, archive, command.Request.Bundle)
	digest := sha256.Sum256(archive)
	require.Equal(t, hex.EncodeToString(digest[:]), command.Request.BundleSHA256)
	require.Equal(t, dep.Id.String(), command.Request.ID)
	require.Equal(t, dep.DeploymentEnvUuid.String(), command.Request.EnvironmentID)
	require.Equal(t, "kubernetes", command.Request.Backend)
	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(signed.Payload, &decoded))
	var steps []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(decoded["steps"], &steps))
	require.NotContains(t, steps[0], "phase", "private Runner wire rejects product-only step phase")
	definition, err = model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	raw, err = dispatcher.signCommand(dep, *definition, archive, json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &envelope))
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	require.NoError(t, json.Unmarshal(signed.Payload, &command))
	require.Equal(t, "apply", command.Strategy)
	require.Empty(t, command.Approval.Mode)
	require.Empty(t, command.Steps)
}

func TestPublishedPipelineProducerDeclaresCapabilitiesRequiredByImporter(t *testing.T) {
	version := publishedRecipeFixture(t)
	definition, err := publishedPipeline(version, model.DeploymentModeDeployPlan)
	require.NoError(t, err)
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: &Server{RunnerTokenSalt: "fixture-salt"}, Key: private}
	dep := &model.DeploymentSummary{Id: uuid.New(), OrgId: "org", RunnerId: "site", DeploymentEnvUuid: uuid.New()}
	_, err = dispatcher.signCommand(dep, *definition, []byte("closure"), json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.ErrorContains(t, err, "not qualified", "a persisted recipe cannot dispatch without explicit negotiated authority")
	_, err = pipelineForRunner(recipeRunner(t, "pipeline.recipe.v1", "local.exec", "artifacts.read", "database.local"), model.DeploymentModeDeployPlan, definition)
	require.NoError(t, err)
	raw, err := dispatcher.signCommand(dep, *definition, []byte("closure"), json.RawMessage(`{}`), "kubernetes", &pipelinePlanBase{BaseDeploymentID: uuid.Nil.String()}, nil, "")
	require.NoError(t, err)
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(raw, &envelope))
	var signed struct {
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(signed.Payload, &command))
	require.ElementsMatch(t, []string{"pipeline.recipe.v1", "local.exec", "artifacts.read", "database.local"}, command.RequiredCapabilities)
	require.Equal(t, version.PipelineId.String(), command.Recipe.ID)
	require.Equal(t, 3, command.Recipe.Version)
	require.Equal(t, version.Digest, command.Recipe.Digest)
	require.Equal(t, expectedLocalApprovalMode, command.Approval.Mode)
	require.Equal(t, "3", command.Approval.PolicyVersion)
	require.Len(t, command.Steps, 3)
}

func TestNativeObservationCommandCannotCarryDeploymentPipelineEffects(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: &Server{RunnerTokenSalt: "test"}, Key: key, MetadataKey: "must-not-enter-observation", ToolsDigest: "qualified-tools"}
	dep := &model.DeploymentSummary{Id: uuid.New(), OrgId: "org", RunnerId: "site", DeploymentEnvUuid: uuid.New(), Mode: model.DeploymentModeObservation}
	definition := model.PipelineDefinition{Strategy: "observation", Steps: []model.PipelineStep{}, RequiredCapabilities: []string{"environment-observation/v1"}}
	encoded, err := dispatcher.signCommand(dep, definition, []byte("immutable baseline archive"), json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.NoError(t, err)
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(encoded, &envelope))
	var signed struct {
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	payload, err := model.FrozenPipelinePayload(encoded, dep)
	require.NoError(t, err)
	require.Equal(t, []byte(signed.Payload), []byte(payload))
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(signed.Payload, &command))
	require.Equal(t, "observation", command.Strategy)
	require.Equal(t, "observation", command.Request.Mode)
	require.Equal(t, definition.RequiredCapabilities, command.RequiredCapabilities)
	require.Empty(t, command.Steps)
	require.Empty(t, command.Approval.Mode)
	require.Nil(t, command.Recipe)
	require.Nil(t, command.Cost)
	require.Nil(t, command.PlanBase)
	require.Nil(t, command.SourcePlan)
	require.Empty(t, command.Request.MetadataKey)
	require.Empty(t, command.Request.EncryptedOutputsRecipient)
}

func TestPipelineDispatcherRequiresDigestOnlyForConfiguredLegacyAdapter(t *testing.T) {
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	file := t.TempDir() + "/signing-key"
	require.NoError(t, os.WriteFile(file, []byte(base64.StdEncoding.EncodeToString(key)), 0o600))
	for _, trial := range []struct {
		legacy  bool
		digest  string
		allowed bool
	}{
		{false, "", true}, {false, strings.Repeat("a", 64), true}, {false, "not-a-digest", false},
		{true, "", false}, {true, strings.Repeat("a", 64), true}, {true, "not-a-digest", false},
	} {
		server := &Server{}
		if trial.legacy {
			server.RunnerRollouts = &RunnerRolloutService{}
		}
		_, err := NewPipelineDispatcher(server, nil, file, trial.digest, "metadata")
		if trial.allowed {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}
