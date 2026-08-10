package runnerresulthandler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hmessaging"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
)

const (
	EventTypeDeploymentResult = "deployment-result"
	EventTypeRunnerError      = "runner-error"
	DeploymentResultSubjects  = "po.v1.orgs.*.runners.*.events.deployment-result"
	RunnerErrorSubjects       = "po.v1.orgs.*.runners.*.events.runner-error"
)

type RunnerEventApplier interface {
	ApplyRunnerDeploymentResult(
		ctx context.Context,
		orgID string,
		runnerID string,
		deploymentID uuid.UUID,
		eventID string,
		body api.DeploymentResultsUpdateBody,
	) error
	ApplyRunnerError(ctx context.Context, orgID, runnerID string, deploymentID uuid.UUID, eventID, code, message string) error
}

type Handler struct {
	Applier RunnerEventApplier
}

type runnerErrorPayload struct {
	JobID     string `json:"job_id"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (h *Handler) Handle(ctx context.Context, logger *zap.Logger, delivery hmessaging.Delivery) error {
	var envelope hmessaging.EventEnvelope
	if err := json.Unmarshal(delivery.Data, &envelope); err != nil {
		return hmessaging.NewTerminalError(errors.Wrap(err, "invalid runner event envelope"))
	}
	if envelope.ProtocolVersion != hmessaging.ProtocolVersionV1 {
		return hmessaging.NewTerminalError(fmt.Errorf("unsupported runner event protocol version %q", envelope.ProtocolVersion))
	}
	if envelope.Type != EventTypeDeploymentResult && envelope.Type != EventTypeRunnerError {
		return hmessaging.NewTerminalError(fmt.Errorf("unexpected runner event type %q", envelope.Type))
	}
	if envelope.EventID == "" {
		return hmessaging.NewTerminalError(errors.New("runner event id must not be empty"))
	}
	expectedSubject, err := hmessaging.RunnerEventSubject(envelope.OrganizationID, envelope.RunnerID, envelope.Type)
	if err != nil {
		return hmessaging.NewTerminalError(errors.Wrap(err, "invalid runner event subject identity"))
	}
	if delivery.Subject != expectedSubject {
		return hmessaging.NewTerminalError(fmt.Errorf("runner event subject %q does not match envelope identity", delivery.Subject))
	}
	deploymentID, err := uuid.Parse(envelope.DeploymentID)
	if err != nil {
		return hmessaging.NewTerminalError(errors.Wrap(err, "invalid deployment id in runner event"))
	}
	if h.Applier == nil {
		return errors.New("runner event applier is not configured")
	}
	switch envelope.Type {
	case EventTypeDeploymentResult:
		var result api.DeploymentResultsUpdateBody
		if err := json.Unmarshal(envelope.Payload, &result); err != nil {
			return hmessaging.NewTerminalError(errors.Wrap(err, "invalid deployment result payload"))
		}
		if !result.Status.Valid() {
			return hmessaging.NewTerminalError(fmt.Errorf("invalid deployment result status %q", result.Status))
		}
		if err := h.Applier.ApplyRunnerDeploymentResult(ctx, envelope.OrganizationID, envelope.RunnerID, deploymentID, envelope.EventID, result); err != nil {
			return errors.Wrap(err, "failed to apply deployment result")
		}
	case EventTypeRunnerError:
		var runnerError runnerErrorPayload
		if err := json.Unmarshal(envelope.Payload, &runnerError); err != nil {
			return hmessaging.NewTerminalError(errors.Wrap(err, "invalid runner error payload"))
		}
		if runnerError.JobID != "" && runnerError.JobID != envelope.DeploymentID {
			return hmessaging.NewTerminalError(errors.New("runner error job id does not match deployment id"))
		}
		if runnerError.Code == "" || runnerError.Message == "" {
			return hmessaging.NewTerminalError(errors.New("runner error code and message must not be empty"))
		}
		if runnerError.Retryable {
			logger.Warn("runner reported a retryable command error", zap.String("event_id", envelope.EventID), zap.String("code", runnerError.Code))
			return nil
		}
		if err := h.Applier.ApplyRunnerError(ctx, envelope.OrganizationID, envelope.RunnerID, deploymentID, envelope.EventID, runnerError.Code, runnerError.Message); err != nil {
			return errors.Wrap(err, "failed to apply runner error")
		}
	}
	logger.Info("applied runner event", zap.String("event_id", envelope.EventID), zap.String("event_type", envelope.Type))
	return nil
}
