package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestPipelineControlSignedWireBindsCapturedAuthority(t *testing.T) {
	public, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now().UTC()
	r := model.PipelineControlRequest{
		Version:         model.PipelineControlVersion,
		ID:              uuid.NewString(),
		RolloutID:       uuid.NewString(),
		OrgID:           "org",
		EnvironmentID:   uuid.NewString(),
		Target:          *nativeTargetFixture(),
		ExecutionDigest: strings.Repeat("c", 64),
		ActorID:         uuid.NewString(),
		Action:          "approve",
		Subject:         strings.Repeat("d", 64),
		IssuedAt:        now,
		ExpiresAt:       now.Add(time.Hour),
	}
	p := PipelineDispatcher{Key: key}
	receipt, err := p.freezeControl(r, "same-key", "intent")
	require.NoError(t, err)
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(receipt.Command, &envelope))
	var signed struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	require.NoError(t, err)
	require.True(t, ed25519.Verify(public, signed.Payload, signature))
	require.Equal(t, receipt.RequestDigest, pipelineControlDigest(signed.Payload))
	var captured model.PipelineControlRequest
	require.NoError(t, json.Unmarshal(signed.Payload, &captured))
	require.Equal(t, r, captured)
	route, err := hmessaging.NativeRunnerToken(r.Target.RunnerUuid.String())
	require.NoError(t, err)
	require.Equal(t, route, envelope.RunnerID)
	require.Equal(t, "pipeline-control:"+r.ID, envelope.CommandID)
	require.Equal(t, r.RolloutID, envelope.DeploymentID)
	require.Equal(t, r.ExpiresAt, envelope.ExpiresAt)
	require.NotContains(t, string(receipt.Command), "deployment_token")
	require.LessOrEqual(t, len(receipt.Command), model.PipelineControlMaxBytes)
	// The bound is the complete transport envelope, including its signature.
	r.FailedStep = strings.Repeat("x", model.PipelineControlMaxBytes-len(receipt.Command))
	exact, err := p.freezeControl(r, "same-key", "intent")
	require.NoError(t, err)
	require.Len(t, exact.Command, model.PipelineControlMaxBytes)
	r.FailedStep += "x"
	_, err = p.freezeControl(r, "same-key", "intent")
	require.ErrorContains(t, err, "8 KiB")
}

func TestPipelineControlIntentAndStrictMetadataShapes(t *testing.T) {
	for _, trial := range []struct {
		body  string
		valid bool
	}{
		{`{"action":"cancel"}`, true},
		{`{"action":"cancel","Action":"approve"}`, false},
		{`{"action":"cancel","action":"deny"}`, false},
		{`{"action":"cancel","actor_id":"spoof"}`, false},
		{`{"action":"cancel","subject failed_step":"ignored"}`, false},
		{`{"action":"cancel","subject\u0020failed_step":"ignored"}`, false},
		{`{"action subject":"cancel"}`, false},
		{`{"action\u0020subject":"cancel"}`, false},
		{`{"action":"cancel","subject":null}`, false},
		{`{"action":"cancel"} {}`, false},
		{`{}`, false},
	} {
		require.Equal(t, trial.valid, pipelineControlObject([]byte(trial.body), "action subject failed_step revision", "action"), trial.body)
	}
	for _, intent := range []pipelineControlIntent{
		{Action: "cancel", Subject: strings.Repeat("a", 64)},
		{Action: "approve"}, {Action: "deny", Subject: strings.Repeat("a", 64), Revision: 1},
		{Action: "repair", FailedStep: "post", Revision: 0},
		{Action: "repair", Subject: strings.Repeat("a", 64), FailedStep: "post", Revision: 3},
	} {
		require.False(t, intent.valid(), intent)
	}
	require.True(t, (pipelineControlIntent{Action: "repair", FailedStep: "post", Revision: 3}).valid())
}

func TestPipelineControlExpiredReceiptRemainsPending(t *testing.T) {
	now := time.Now().UTC()
	receipt := &model.PipelineControl{ID: uuid.New(), DeploymentID: uuid.New(), ActorID: uuid.New(), Action: "cancel", CreatedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-time.Hour)}
	view, err := pipelineControlView(receipt)
	require.NoError(t, err)
	require.Equal(t, DeploymentPipelineControlReceiptStatusPending, view.Status)
	require.Nil(t, view.Result, "expiry never fabricates a Runner refusal")
}
