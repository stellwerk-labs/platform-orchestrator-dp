package runnerresulthandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

type recordingApplier struct {
	errorCalls  int
	resultCalls int
	code        string
	message     string
}

func (a *recordingApplier) ApplyRunnerDeploymentResultEnvelope(context.Context, hmessaging.EventEnvelope, api.DeploymentResultsUpdateBody) error {
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

func TestApplicationErrorRetriesOnlyRecoverableResults(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		terminal bool
	}{
		{"deleted target", fmt.Errorf("%w: %w", api.ErrRunnerEventTargetInvalid, model.NewErrNotFound("deleted deployment")), true},
		{"completed target", fmt.Errorf("%w: %w", api.ErrRunnerEventTargetInvalid, model.NewErrConflict("deployment already completed")), true},
		{"unclassified persistence absence", model.NewErrNotFound("other persistence record"), false},
		{"unclassified transaction conflict", model.NewErrConflict("concurrent transaction"), false},
		{"database unavailable", errors.New("database unavailable"), false},
		{"IAM unavailable", errors.New("IAM unavailable"), false},
		{"cancelled request", context.Canceled, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := applicationError(test.err, "failed to apply result")
			assert.Equal(t, test.terminal, hmessaging.IsTerminalError(err))
			require.ErrorIs(t, err, test.err)
		})
	}
}

func TestNativeFramingRequiresScopedDeliveryAndRejectsLegacyReservedIDs(t *testing.T) {
	token, err := hmessaging.NativeRunnerToken("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	require.NoError(t, err)
	for _, test := range []struct {
		name, runner, inner, outer, kind string
		accepted                         bool
	}{
		{"native scoped", token, "inner", "scoped", EventTypeDeploymentResult, true},
		{"native raw", token, "inner", "inner", EventTypeDeploymentResult, false},
		{"native foreign scoped", token, "inner", "foreign", EventTypeDeploymentResult, false},
		{"native error", token, "inner", "scoped", EventTypeRunnerError, false},
		{"legacy reserved inner", "runner-1", hmessaging.NativeMessageIDPrefix + "x", "inner", EventTypeDeploymentResult, false},
		{"legacy reserved outer", "runner-1", "inner", hmessaging.NativeMessageIDPrefix + "x", EventTypeDeploymentResult, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			applier := new(recordingApplier)
			handler := &Handler{Applier: applier}
			delivery := runnerEventDelivery(t, test.kind, map[string]any{"status": "success", "code": "failed", "message": "failed"})
			var envelope hmessaging.EventEnvelope
			require.NoError(t, json.Unmarshal(delivery.Data, &envelope))
			envelope.RunnerID, envelope.EventID = test.runner, test.inner
			delivery.ID = test.outer
			if test.outer == "scoped" || test.outer == "foreign" {
				scope := token
				if test.outer == "foreign" {
					scope, err = hmessaging.NativeRunnerToken("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
					require.NoError(t, err)
				}
				delivery.ID, err = hmessaging.NativeMessageID(envelope.OrganizationID, scope, "events", envelope.Type, envelope.EventID)
				require.NoError(t, err)
			}
			delivery.Subject, err = hmessaging.RunnerEventSubject(envelope.OrganizationID, envelope.RunnerID, envelope.Type)
			require.NoError(t, err)
			delivery.Data, err = json.Marshal(envelope)
			require.NoError(t, err)
			err = handler.Handle(t.Context(), zaptest.NewLogger(t), delivery)
			if test.accepted {
				require.NoError(t, err)
				require.Equal(t, 1, applier.resultCalls)
			} else {
				require.Error(t, err)
				require.True(t, hmessaging.IsTerminalError(err))
				require.Zero(t, applier.resultCalls)
				require.Zero(t, applier.errorCalls)
			}
		})
	}
}

// This callback checks transport routing only. Retained receipt authority is
// exercised by the real PostgreSQL API tests, not supplied by this fixture.
func TestHandleNativeControlResultRequiresCapturedTransportIdentity(t *testing.T) {
	route, err := hmessaging.NativeRunnerToken(uuid.NewString())
	require.NoError(t, err)
	id := uuid.NewString()
	event := hmessaging.EventEnvelope{
		ProtocolVersion: hmessaging.ProtocolVersionV1,
		EventID:         "pipeline-control:" + id + ":result",
		CommandID:       "pipeline-control:" + id,
		OrganizationID:  "org",
		RunnerID:        route,
		DeploymentID:    uuid.NewString(),
		Type:            EventTypeControlResult,
		CreatedAt:       time.Now().UTC(),
		Payload:         json.RawMessage(`{}`),
	}
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	subject, err := hmessaging.RunnerEventSubject(event.OrganizationID, route, event.Type)
	require.NoError(t, err)
	messageID, err := hmessaging.NativeMessageID(event.OrganizationID, route, "events", event.Type, event.EventID)
	require.NoError(t, err)
	delivery := hmessaging.Delivery{Message: hmessaging.Message{ID: messageID, Subject: subject, Data: raw}}
	called := 0
	handler := Handler{ControlResult: func(_ context.Context, data []byte) error { called++; require.Equal(t, raw, data); return nil }}
	invalid := delivery
	invalid.ID = event.EventID
	require.Error(t, handler.Handle(t.Context(), zaptest.NewLogger(t), invalid))
	require.Zero(t, called)
	invalid = delivery
	invalid.Subject += ".foreign"
	require.Error(t, handler.Handle(t.Context(), zaptest.NewLogger(t), invalid))
	require.Zero(t, called)
	require.NoError(t, handler.Handle(t.Context(), zaptest.NewLogger(t), delivery))
	require.Equal(t, 1, called)
}
