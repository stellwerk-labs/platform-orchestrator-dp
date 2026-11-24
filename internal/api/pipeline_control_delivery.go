package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"go.uber.org/zap"
)

// DispatchControl reads retained command bytes only. It never obtains another
// execution reservation, Runner registration, token or Core admission decision.
func (p *PipelineDispatcher) DispatchControl(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error {
	var event struct {
		OrgID        string    `json:"org_id"`
		DeploymentID uuid.UUID `json:"deployment_id"`
		ControlID    uuid.UUID `json:"control_id"`
	}
	if d.Subject != model.PipelineControlOutboxSubject || !pipelineControlObject(d.Data, "org_id deployment_id control_id", "org_id deployment_id control_id") || json.Unmarshal(d.Data, &event) != nil {
		return hmessaging.NewTerminalError(errors.New("invalid control outbox event"))
	}
	tx, err := p.Server.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := model.ReadPipelineControl(ctx, tx, event.OrgID, event.DeploymentID, event.ControlID)
	if err != nil {
		return err
	}
	route, err := hmessaging.NativeRunnerToken(receipt.RunnerUUID.String())
	if err != nil {
		return err
	}
	subject, err := hmessaging.RunnerCommandSubject(receipt.OrgID, route)
	if err != nil {
		return err
	}
	id, err := hmessaging.NativeMessageID(receipt.OrgID, route, "commands", model.PipelineControlCommandType, "pipeline-control:"+receipt.ID.String())
	if err != nil {
		return err
	}
	if err = p.Publisher.Publish(ctx, hmessaging.Message{ID: id, Subject: subject, Data: receipt.Command, CreatedAt: receipt.CreatedAt, ExpiresAt: receipt.ExpiresAt}); err != nil {
		return hmessaging.NewRetryError(err)
	}
	logger.Info("published retained Native control")
	return nil
}

// ApplyPipelineControlResult validates retained control authority independently
// of Deployment existence and mutates only that receipt. Expiry is uncertainty,
// never a fabricated refusal, execution completion or NoEffect decision.
func (s *Server) ApplyPipelineControlResult(ctx context.Context, raw []byte) error {
	invalid := ErrRunnerEventTargetInvalid
	var event hmessaging.EventEnvelope
	var result model.PipelineControlResult
	if len(raw) == 0 || len(raw) > model.PipelineControlMaxBytes ||
		!pipelineControlObject(raw, "protocol_version event_id command_id organization_id runner_id deployment_id type created_at payload", "protocol_version event_id command_id organization_id runner_id deployment_id type created_at payload") ||
		json.Unmarshal(raw, &event) != nil {
		return invalid
	}
	if !pipelineControlObject(event.Payload, "version control_id rollout_id request_digest execution_digest outcome code phase revision", "version control_id rollout_id request_digest execution_digest outcome code phase revision") || json.Unmarshal(event.Payload, &result) != nil {
		return invalid
	}
	id, err := uuid.Parse(result.ControlID)
	if err != nil || id == uuid.Nil || id.String() != result.ControlID {
		return invalid
	}
	dep, err := uuid.Parse(result.RolloutID)
	if err != nil || dep == uuid.Nil || dep.String() != result.RolloutID || !validPipelineControlResult(result) {
		return invalid
	}
	if event.ProtocolVersion != hmessaging.ProtocolVersionV1 || event.Type != model.PipelineControlEventType || event.DeploymentID != result.RolloutID || event.CommandID != "pipeline-control:"+result.ControlID || event.EventID != event.CommandID+":result" || event.CreatedAt.IsZero() {
		return invalid
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := model.LockPipelineControl(ctx, tx, event.OrganizationID, dep, id)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return invalid
		}
		return err
	}

	route, err := hmessaging.NativeRunnerToken(receipt.RunnerUUID.String())
	if err != nil || event.RunnerID != route || result.RequestDigest != receipt.RequestDigest || result.ExecutionDigest != receipt.ExecutionDigest {
		return invalid
	}
	digest := pipelineControlDigest(raw)
	if receipt.ResultEventDigest.Valid {
		if receipt.ResultEventDigest.String != digest || !bytes.Equal(receipt.Result, event.Payload) {
			return invalid
		}
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_controls SET result=$2,result_event_digest=$3,resolved_at=now() WHERE id=$1`, id, []byte(event.Payload), digest)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func validPipelineControlResult(r model.PipelineControlResult) bool {
	return r.Version == model.PipelineControlResultVersion && nativeToolsDigest.MatchString(r.RequestDigest) && nativeToolsDigest.MatchString(r.ExecutionDigest) && (r.Outcome == "applied" || r.Outcome == "refused") && (r.Outcome == "refused") == (r.Code != "") && len(r.Code) <= 128 &&
		r.Phase != "" &&
		len(r.Phase) <= 64 &&
		r.Revision > 0
}

// Inspect names before projecting values; encoding/json otherwise accepts
// duplicate and case-aliased identities using last-member wins.
func pipelineControlObject(raw []byte, allowed, required string) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	allowedNames := strings.Fields(allowed)
	seen := map[string]bool{}
	for decoder.More() {
		token, err = decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] || !slices.Contains(allowedNames, name) {
			return false
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil || bytes.Equal(value, []byte("null")) {
			return false
		}
		seen[name] = true
	}
	if _, err = decoder.Token(); err != nil || decoder.Decode(new(any)) != io.EOF {
		return false
	}
	for _, name := range strings.Fields(required) {
		if !seen[name] {
			return false
		}
	}
	return true
}
