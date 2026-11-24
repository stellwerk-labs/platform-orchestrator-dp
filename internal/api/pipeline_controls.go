package api

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const PipelineControlsPath = CanonicalPipelinePath + "/controls"

type pipelineControlIntent struct {
	Action     string `json:"action"`
	Subject    string `json:"subject"`
	FailedStep string `json:"failed_step"`
	Revision   int64  `json:"revision"`
}

func pipelineControlDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (i pipelineControlIntent) valid() bool {
	switch i.Action {
	case "approve", "deny":
		return nativeToolsDigest.MatchString(i.Subject) && i.FailedStep == "" && i.Revision == 0
	case runnerRolloutCancel:
		return i.Subject == "" && i.FailedStep == "" && i.Revision == 0
	case runnerRolloutRepair:
		return i.Subject == "" && i.FailedStep != "" && len(i.FailedStep) <= 128 && i.Revision > 0
	default:
		return false
	}
}

func (s *Server) CreateDeploymentPipelineControl(ctx context.Context, r CreateDeploymentPipelineControlRequestObject) (CreateDeploymentPipelineControlResponseObject, error) {
	actor, err := pipelineCaller(ctx)
	if err != nil {
		return nil, err
	}
	if err = s.validatePipelinePlanAction(ctx, r.Params.IdempotencyKey); err != nil {
		return nil, err
	}
	if r.Body == nil {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "control body required")
	}
	raw, err := json.Marshal(r.Body)
	if err != nil {
		return nil, err
	}
	var intent pipelineControlIntent
	if json.Unmarshal(raw, &intent) != nil || !intent.valid() {
		return nil, echo.NewHTTPError(http.StatusBadRequest, "invalid control authority")
	}
	receipt, err := s.acceptPipelineControl(ctx, r.OrgId, r.DeploymentId, actor, r.Params.IdempotencyKey, intent)
	if err != nil {
		return nil, pipelineHTTPError(err)
	}
	view, err := pipelineControlView(receipt)
	return CreateDeploymentPipelineControl202JSONResponse(view), err
}

func (s *Server) GetDeploymentPipelineControl(ctx context.Context, r GetDeploymentPipelineControlRequestObject) (GetDeploymentPipelineControlResponseObject, error) {
	actor, err := pipelineCaller(ctx)
	if err != nil {
		return nil, err
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := model.ReadPipelineControl(ctx, tx, r.OrgId, r.DeploymentId, r.ControlId)
	if err != nil {
		return nil, pipelineHTTPError(err)
	}
	if err = s.authorizePipelineControl(ctx, actor, receipt, false); err != nil {
		return nil, err
	}
	view, err := pipelineControlView(receipt)
	return GetDeploymentPipelineControl200JSONResponse(view), err
}

func pipelineControlView(r *model.PipelineControl) (DeploymentPipelineControlReceipt, error) {
	var view DeploymentPipelineControlReceipt
	status := DeploymentPipelineControlReceiptStatusPending
	if len(r.Result) > 0 {
		status = DeploymentPipelineControlReceiptStatusResolved
	}
	raw, err := json.Marshal(struct {
		*model.PipelineControl
		Status DeploymentPipelineControlReceiptStatus `json:"status"`
	}{r, status})
	if err != nil {
		return view, err
	}
	err = json.Unmarshal(raw, &view)
	return view, err
}

func (s *Server) authorizePipelineControl(ctx context.Context, actor uuid.UUID, r *model.PipelineControl, write bool) error {
	if err := s.checkEnvAuthorization(ctx, actor, r.OrgID, r.EnvironmentID, PermissionDeploymentRead); err != nil {
		return err
	}
	if write {
		return s.checkEnvAuthorization(ctx, actor, r.OrgID, r.EnvironmentID, PermissionDeploymentWrite)
	}
	return nil
}

func (s *Server) acceptPipelineControl(ctx context.Context, org string, id, actor uuid.UUID, key string, intent pipelineControlIntent) (*model.PipelineControl, error) {
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	// Check retained authority before the owning Deployment, which may have
	// aged out. Every replay still checks current permission on its Environment.
	raw, _ := json.Marshal(intent)
	digest := pipelineControlDigest(raw)
	prior, err := model.FindPipelineControl(ctx, tx, org, id, actor, key)
	if err == nil {
		return s.replayPipelineControl(ctx, actor, prior, digest)
	}
	if _, missing := model.IsErrNotFound(err); !missing {
		return nil, err
	}
	dep, _, _, _, err := s.Database.GetDeployment(ctx, tx, org, id, model.GetModeDefault)
	if err != nil {
		return nil, err
	}
	authority := &model.PipelineControl{OrgID: org, EnvironmentID: dep.DeploymentEnvUuid}
	if err = s.authorizePipelineControl(ctx, actor, authority, true); err != nil {
		return nil, err
	}
	var locked uuid.UUID
	if err = tx.QueryRowContext(ctx, `SELECT d.id FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1 AND e.org_id=$2 FOR UPDATE OF d`, id, org).Scan(&locked); err != nil {
		return nil, err
	}
	prior, err = model.FindPipelineControl(ctx, tx, org, id, actor, key)
	if err == nil {
		return s.replayPipelineControl(ctx, actor, prior, digest)
	}
	if _, missing := model.IsErrNotFound(err); !missing {
		return nil, err
	}
	dep, _, _, _, err = s.Database.GetDeployment(ctx, tx, org, id, model.GetModeDefault)
	if err != nil {
		return nil, err
	}
	receipt, err := s.freezeAcceptedPipelineControl(ctx, tx, dep, actor, key, intent, digest)
	if err != nil {
		return nil, err
	}
	if err = model.InsertPipelineControl(ctx, tx, receipt); err != nil {
		return nil, err
	}
	event, err := json.Marshal(struct {
		OrgID        string    `json:"org_id"`
		DeploymentID uuid.UUID `json:"deployment_id"`
		ControlID    uuid.UUID `json:"control_id"`
	}{org, id, receipt.ID})
	if err != nil {
		return nil, err
	}
	messages, err := s.Database.InsertPendingEventMessages(ctx, tx, []*hstandardoutbox.PendingEventMessage{{Subject: model.PipelineControlOutboxSubject, Payload: event}})
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	reliableoutbox.OptimisticPublish(ctx, s.Logger, s.Database.AsReliableOutboxStore(), s.EventPublisher, messages)
	return receipt, nil
}

func (s *Server) freezeAcceptedPipelineControl(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, actor uuid.UUID, key string, intent pipelineControlIntent, digest string) (*model.PipelineControl, error) {
	id, org := dep.Id, dep.OrgId
	if dep.NativeTarget == nil || !dep.HasPipeline || dep.HasNativeObservation {
		return nil, model.NewErrConflict("signed controls require an accepted Native pipeline")
	}
	var command []byte
	if err := tx.QueryRowContext(ctx, `SELECT command FROM deployment_pipeline_runs WHERE deployment_id=$1 FOR SHARE`, id).Scan(&command); err != nil {
		return nil, err
	}
	payload, err := model.FrozenPipelinePayload(command, dep)
	if err != nil {
		return nil, model.NewErrConflict("accepted execution authority is unavailable")
	}
	var original struct {
		IssuedAt  time.Time `json:"issued_at"`
		ExpiresAt time.Time `json:"expires_at"`
	}
	if json.Unmarshal(payload, &original) != nil || original.IssuedAt.IsZero() || !original.ExpiresAt.After(original.IssuedAt) || original.ExpiresAt.Sub(original.IssuedAt) > 7*24*time.Hour {
		return nil, model.NewErrConflict("accepted execution budget expired")
	}
	if s.PipelineDispatcher == nil || len(s.PipelineDispatcher.Key) != ed25519.PrivateKeySize {
		return nil, echo.NewHTTPError(http.StatusServiceUnavailable, "signed control dispatch is unavailable")
	}
	now := time.Now().UTC()
	if now.Before(original.IssuedAt) || intent.Action != runnerRolloutRepair && !original.ExpiresAt.After(now) {
		return nil, model.NewErrConflict("accepted execution budget is unavailable")
	}
	expiry := original.ExpiresAt
	if intent.Action == runnerRolloutRepair {
		expiry = now.Add(pipelineCommandTTL)
	}
	request := model.PipelineControlRequest{
		Version:         model.PipelineControlVersion,
		ID:              uuid.NewString(),
		RolloutID:       id.String(),
		OrgID:           org,
		EnvironmentID:   dep.DeploymentEnvUuid.String(),
		Target:          *dep.NativeTarget,
		ExecutionDigest: pipelineControlDigest(payload),
		ActorID:         actor.String(),
		Action:          intent.Action,
		Subject:         intent.Subject,
		FailedStep:      intent.FailedStep,
		Revision:        intent.Revision,
		IssuedAt:        now,
		ExpiresAt:       expiry,
	}
	return s.PipelineDispatcher.freezeControl(request, key, digest)
}

func (s *Server) replayPipelineControl(ctx context.Context, actor uuid.UUID, prior *model.PipelineControl, digest string) (*model.PipelineControl, error) {
	if err := s.authorizePipelineControl(ctx, actor, prior, true); err != nil {
		return nil, err
	}
	if prior.IntentDigest != digest {
		return nil, model.NewErrConflict("Idempotency-Key already binds a different control intent")
	}
	return prior, nil
}

func (p *PipelineDispatcher) freezeControl(r model.PipelineControlRequest, key, intent string) (*model.PipelineControl, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	signed, err := json.Marshal(struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}{payload, base64.StdEncoding.EncodeToString(ed25519.Sign(p.Key, payload))})
	if err != nil {
		return nil, err
	}
	route, err := hmessaging.NativeRunnerToken(r.Target.RunnerUuid.String())
	if err != nil {
		return nil, err
	}
	command, err := json.Marshal(
		hmessaging.CommandEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, CommandID: "pipeline-control:" + r.ID, OrganizationID: r.OrgID, RunnerID: route, DeploymentID: r.RolloutID, Type: model.PipelineControlCommandType, CreatedAt: r.IssuedAt, ExpiresAt: r.ExpiresAt, Payload: signed},
	)
	if err != nil {
		return nil, err
	}
	if len(command) > model.PipelineControlMaxBytes {
		return nil, model.NewErrBadRequest("signed control exceeds 8 KiB")
	}
	return &model.PipelineControl{
		ID:              uuid.MustParse(r.ID),
		OrgID:           r.OrgID,
		DeploymentID:    uuid.MustParse(r.RolloutID),
		EnvironmentID:   uuid.MustParse(r.EnvironmentID),
		RunnerUUID:      r.Target.RunnerUuid,
		ActorID:         uuid.MustParse(r.ActorID),
		ClientKey:       key,
		IntentDigest:    intent,
		RequestDigest:   pipelineControlDigest(payload),
		ExecutionDigest: r.ExecutionDigest,
		Action:          r.Action,
		Command:         command,
		CreatedAt:       r.IssuedAt,
		ExpiresAt:       r.ExpiresAt,
	}, nil
}
