package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
)

const nativeProgressPayloadLimit = 1 << 20

func (s *Server) ApplyPipelineProgressEnvelope(ctx context.Context, envelope hmessaging.EventEnvelope) error {
	id, err := uuid.Parse(envelope.DeploymentID)
	if err != nil {
		return fmt.Errorf("%w: invalid pipeline identity", ErrRunnerEventTargetInvalid)
	}
	var event struct {
		ID       int64                  `json:"id"`
		Progress model.PipelineProgress `json:"progress"`
	}
	if json.Unmarshal(envelope.Payload, &event) != nil || event.Progress.ID != id.String() || event.Progress.Revision < 1 {
		return fmt.Errorf("%w: invalid pipeline progress", ErrRunnerEventTargetInvalid)
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	dep, _, _, _, err := s.Database.GetDeployment(ctx, tx, envelope.OrganizationID, id, model.GetModeForUpdate)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return fmt.Errorf("%w: pipeline target missing", ErrRunnerEventTargetInvalid)
		}
		return err
	}
	if _, err := runnerEventInboxID(dep, &RunnerEventMetadata{EventID: envelope.EventID, RunnerID: envelope.RunnerID, Type: "rollout-progress", Envelope: &envelope}); err != nil {
		return err
	}
	if event.Progress.PostRepair != nil && dep.NativeTarget == nil {
		return fmt.Errorf("%w: post repair requires native progress", ErrRunnerEventTargetInvalid)
	}
	if dep.NativeTarget != nil && envelope.EventID != dep.Id.String()+":"+strconv.FormatInt(event.Progress.Revision, 10) {
		return fmt.Errorf("%w: invalid native progress revision identity", ErrRunnerEventTargetInvalid)
	}
	if dep.DeploymentEnvUuid.String() != event.Progress.EnvironmentID {
		return fmt.Errorf("%w: pipeline target mismatch", ErrRunnerEventTargetInvalid)
	}
	if dep.NativeTarget != nil && event.Progress.LogCapture != nil {
		var wire struct {
			Progress map[string]json.RawMessage `json:"progress"`
		}
		if nativelogs.StrictJSON(envelope.Payload, new(any), nativeProgressPayloadLimit) != nil || json.Unmarshal(envelope.Payload, &wire) != nil {
			return invalidNativeLog()
		}
		descriptor, parseErr := nativelogs.ParseCapture(wire.Progress["log_capture"])
		if parseErr != nil {
			return invalidNativeLog()
		}
		if _, err = nativeLogCommand(ctx, tx, dep, descriptor, &event.Progress.Subject); err != nil {
			return err
		}
		if descriptor.Authority.CaptureRevision > event.Progress.Revision {
			return invalidNativeLog()
		}
	}
	now := time.Now().UTC()
	event.Progress.LastReceivedAt = &now
	if err = model.UpdatePipelineProgress(ctx, tx, event.Progress); err != nil {
		if _, conflict := model.IsErrConflict(err); conflict {
			return fmt.Errorf("%w: %w", ErrRunnerEventTargetInvalid, err)
		}
		return err
	}
	return tx.Commit()
}
