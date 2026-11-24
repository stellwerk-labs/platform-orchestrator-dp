package runnerresulthandler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hmessaging"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
)

const (
	EventTypeDeploymentResult = "deployment-result"
	EventTypeRunnerError      = "runner-error"
	EventTypePipelineProgress = "rollout-progress"
	EventTypeControlResult    = "rollout-control-result"
	ControlResultSubjects     = "po.v1.orgs.*.runners.*.events.rollout-control-result"
	NativeLogReadySubjects    = "po.v1.orgs.*.runners.*.events.log-object-ready"
	PipelineProgressSubjects  = "po.v1.orgs.*.runners.*.events.rollout-progress"
	DeploymentResultSubjects  = "po.v1.orgs.*.runners.*.events.deployment-result"
	RunnerErrorSubjects       = "po.v1.orgs.*.runners.*.events.runner-error"
)

type RunnerEventApplier interface {
	ApplyRunnerDeploymentResultEnvelope(context.Context, hmessaging.EventEnvelope, api.DeploymentResultsUpdateBody) error
	ApplyRunnerError(ctx context.Context, orgID, runnerID string, deploymentID uuid.UUID, eventID, code, message string) error
}

type Handler struct {
	PipelineProgress func(context.Context, hmessaging.EventEnvelope) error
	ControlResult    func(context.Context, []byte) error
	Applier          RunnerEventApplier
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
	if envelope.Type != EventTypeDeploymentResult && envelope.Type != EventTypeRunnerError && envelope.Type != EventTypePipelineProgress && envelope.Type != EventTypeControlResult && envelope.Type != nativelogs.ReadyEventType {
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
	if strings.HasPrefix(envelope.RunnerID, hmessaging.NativeRunnerTokenPrefix) {
		expectedID, identityErr := hmessaging.NativeMessageID(envelope.OrganizationID, envelope.RunnerID, "events", envelope.Type, envelope.EventID)
		if identityErr != nil || delivery.ID != expectedID || envelope.Type == EventTypeRunnerError {
			return hmessaging.NewTerminalError(errors.New("native runner event identity is invalid"))
		}
	} else if strings.HasPrefix(envelope.EventID, hmessaging.NativeMessageIDPrefix) || strings.HasPrefix(delivery.ID, hmessaging.NativeMessageIDPrefix) {
		return hmessaging.NewTerminalError(errors.New("legacy runner event uses a reserved identity"))
	}
	if envelope.Type == EventTypeControlResult {
		if !strings.HasPrefix(envelope.RunnerID, hmessaging.NativeRunnerTokenPrefix) || h.ControlResult == nil {
			return hmessaging.NewTerminalError(errors.New("Native control result is unavailable"))
		}
		if err := h.ControlResult(ctx, delivery.Data); err != nil {
			return applicationError(err, "failed to apply control result")
		}
		return nil
	}
	deploymentID, err := uuid.Parse(envelope.DeploymentID)
	if err != nil {
		return hmessaging.NewTerminalError(errors.Wrap(err, "invalid deployment id in runner event"))
	}
	if h.Applier == nil {
		return errors.New("runner event applier is not configured")
	}
	switch envelope.Type {
	case nativelogs.ReadyEventType:
		if _, _, err := nativelogs.ParseReady(delivery.Data); err != nil {
			return hmessaging.NewTerminalError(err)
		}
		applier, ok := h.Applier.(interface {
			ApplyNativeLogReady(context.Context, hmessaging.EventEnvelope) error
		})
		if !ok {
			return errors.New("native log intake is not configured")
		}
		if err := applier.ApplyNativeLogReady(ctx, envelope); err != nil {
			return applicationError(err, "failed to accept native log reference")
		}
	case EventTypePipelineProgress:
		if h.PipelineProgress == nil {
			return hmessaging.NewTerminalError(errors.New("pipeline progress is not configured"))
		}
		if err := h.PipelineProgress(ctx, envelope); err != nil {
			return applicationError(err, "failed to apply pipeline progress")
		}
	case EventTypeDeploymentResult:
		var result api.DeploymentResultsUpdateBody
		if err := json.Unmarshal(envelope.Payload, &result); err != nil {
			return hmessaging.NewTerminalError(errors.Wrap(err, "invalid deployment result payload"))
		}
		if !result.Status.Valid() {
			return hmessaging.NewTerminalError(fmt.Errorf("invalid deployment result status %q", result.Status))
		}
		if err := h.Applier.ApplyRunnerDeploymentResultEnvelope(ctx, envelope, result); err != nil {
			return applicationError(err, "failed to apply deployment result")
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
			return applicationError(err, "failed to apply runner error")
		}
	}
	logger.Info("applied runner event", zap.String("event_id", envelope.EventID), zap.String("event_type", envelope.Type))
	return nil
}

func applicationError(err error, message string) error {
	err = errors.Wrap(err, message)
	if errors.Is(err, api.ErrRunnerEventTargetInvalid) {
		return hmessaging.NewTerminalError(err)
	}
	return err
}
