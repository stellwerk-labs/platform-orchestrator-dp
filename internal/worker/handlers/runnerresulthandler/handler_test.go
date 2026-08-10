package runnerresulthandler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
)

type recordingApplier struct {
	errorCalls  int
	resultCalls int
	code        string
	message     string
}

func (a *recordingApplier) ApplyRunnerDeploymentResult(context.Context, string, string, uuid.UUID, string, api.DeploymentResultsUpdateBody) error {
	a.resultCalls++
	return nil
}

func (a *recordingApplier) ApplyRunnerError(_ context.Context, _, _ string, _ uuid.UUID, _, code, message string) error {
	a.errorCalls++
	a.code, a.message = code, message
	return nil
}

func runnerEventDelivery(t *testing.T, eventType string, payload any) hmessaging.Delivery {
	t.Helper()
	deploymentID := "11111111-1111-1111-1111-111111111111"
	payloadJSON, err := json.Marshal(payload)
	require.NoError(t, err)
	envelope := hmessaging.EventEnvelope{
		ProtocolVersion: hmessaging.ProtocolVersionV1, EventID: "event-1",
		OrganizationID: "org-1", RunnerID: "runner-1", DeploymentID: deploymentID,
		Type: eventType, CreatedAt: time.Now().UTC(), Payload: payloadJSON,
	}
	data, err := json.Marshal(envelope)
	require.NoError(t, err)
	subject, err := hmessaging.RunnerEventSubject(envelope.OrganizationID, envelope.RunnerID, eventType)
	require.NoError(t, err)
	return hmessaging.Delivery{Message: hmessaging.Message{ID: envelope.EventID, Subject: subject, Data: data}}
}

func TestHandleTerminalRunnerErrorsFailDeployment(t *testing.T) {
	for _, test := range []struct{ code, message string }{
		{"COMMAND_EXPIRED", "command expired before execution"},
		{"REMOTE_RUNNER", "job creation exhausted retries"},
	} {
		t.Run(test.code, func(t *testing.T) {
			applier := new(recordingApplier)
			handler := &Handler{Applier: applier}
			delivery := runnerEventDelivery(t, EventTypeRunnerError, map[string]any{
				"job_id": "11111111-1111-1111-1111-111111111111", "code": test.code,
				"message": test.message, "retryable": false,
			})
			require.NoError(t, handler.Handle(t.Context(), zaptest.NewLogger(t), delivery))
			assert.Equal(t, 1, applier.errorCalls)
			assert.Equal(t, test.code, applier.code)
			assert.Equal(t, test.message, applier.message)
		})
	}
}

func TestHandleRetryableRunnerErrorDoesNotFailDeployment(t *testing.T) {
	applier := new(recordingApplier)
	handler := &Handler{Applier: applier}
	delivery := runnerEventDelivery(t, EventTypeRunnerError, map[string]any{
		"job_id": "11111111-1111-1111-1111-111111111111", "code": "REMOTE_RUNNER",
		"message": "temporary failure", "retryable": true,
	})
	require.NoError(t, handler.Handle(t.Context(), zaptest.NewLogger(t), delivery))
	assert.Zero(t, applier.errorCalls)
}

func TestHandleRejectsMalformedRunnerError(t *testing.T) {
	handler := &Handler{Applier: new(recordingApplier)}
	delivery := runnerEventDelivery(t, EventTypeRunnerError, map[string]any{"retryable": false})
	err := handler.Handle(t.Context(), zaptest.NewLogger(t), delivery)
	require.Error(t, err)
	assert.True(t, hmessaging.IsTerminalError(err))
}

func TestHandleRejectsUnknownDeploymentStatus(t *testing.T) {
	handler := &Handler{Applier: new(recordingApplier)}
	delivery := runnerEventDelivery(t, EventTypeDeploymentResult, map[string]any{"status": "probably-fine"})
	err := handler.Handle(t.Context(), zaptest.NewLogger(t), delivery)
	require.Error(t, err)
	assert.True(t, hmessaging.IsTerminalError(err))
}

func TestHandleRejectsSubjectEnvelopeIdentityMismatch(t *testing.T) {
	handler := &Handler{Applier: new(recordingApplier)}
	delivery := runnerEventDelivery(t, EventTypeRunnerError, map[string]any{
		"code": "COMMAND_EXPIRED", "message": "expired", "retryable": false,
	})
	delivery.Subject = "po.v1.orgs.another-org.runners.runner-1.events.runner-error"
	err := handler.Handle(t.Context(), zaptest.NewLogger(t), delivery)
	require.Error(t, err)
	assert.True(t, hmessaging.IsTerminalError(err))
}
