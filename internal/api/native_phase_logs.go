package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

const nativeLogReadTimeout = 30 * time.Second

var nativeLogExecutionID = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)

func invalidNativeLog() error {
	return fmt.Errorf("%w: native log authority mismatch", ErrRunnerEventTargetInvalid)
}

// Compare only retained Deployment and signed command authority. Current Runner
// assignments and registrations cannot retarget historical evidence.
func readNativeLogCommand(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary) (*pipelineCommand, model.PipelineProgress, string, error) {
	var raw, progressJSON []byte
	if err := tx.QueryRowContext(ctx, `SELECT command,progress FROM deployment_pipeline_runs WHERE deployment_id=$1`, dep.Id).Scan(&raw, &progressJSON); err != nil {
		return nil, model.PipelineProgress{}, "", err
	}
	payload, err := model.FrozenPipelinePayload(raw, dep)
	if err != nil {
		return nil, model.PipelineProgress{}, "", invalidNativeLog()
	}
	var command pipelineCommand
	var progress model.PipelineProgress
	if json.Unmarshal(payload, &command) != nil || json.Unmarshal(progressJSON, &progress) != nil {
		return nil, model.PipelineProgress{}, "", invalidNativeLog()
	}
	return &command, progress, nativelogs.Hash(payload), nil
}

func nativeLogCommand(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, descriptor nativelogs.Descriptor, subjectOverride *string) (*pipelineCommand, error) {
	if dep.NativeTarget == nil || !dep.HasPipeline || descriptor.Validate() != nil {
		return nil, invalidNativeLog()
	}
	command, progress, commandDigest, err := readNativeLogCommand(ctx, tx, dep)
	if err != nil {
		return nil, err
	}
	a, target := descriptor.Authority, dep.NativeTarget
	expectedTarget := nativelogs.Target{
		RunnerID:                 target.RunnerId,
		RunnerUUID:               target.RunnerUuid.String(),
		RuntimeVersion:           string(target.RuntimeVersion),
		PipelineVersion:          string(target.PipelineVersion),
		ToolsDigest:              target.ToolsDigest,
		StateBackendType:         string(target.StateBackendType),
		StateConfigurationDigest: target.StateConfigurationDigest,
	}
	if descriptor.Status == nativelogs.PendingTransfer && command.Request.EncryptedLogsRecipient == "" || descriptor.Status == nativelogs.NotManaged && command.Request.EncryptedLogsRecipient != "" {
		return nil, invalidNativeLog()
	}
	expected := nativelogs.Authority{OrganizationID: dep.OrgId, EnvironmentID: dep.DeploymentEnvUuid.String(), DeploymentID: dep.Id.String(), CommandDigest: commandDigest, Target: expectedTarget, Phase: a.Phase, ExecutionID: dep.Id.String(), CaptureRevision: a.CaptureRevision, Subject: a.Subject}
	if command.SourcePlan != nil {
		expected.SourcePlanID, expected.SourceSubject = command.SourcePlan.SourcePlanID, command.SourcePlan.Subject
		if a.Phase == nativelogs.PlanPhase {
			expected.ExecutionID, expected.Inherited = expected.SourcePlanID, true
		}
	}
	if a != expected || (a.Phase == nativelogs.ApplyPhase && model.IsPlanDeployment(dep.Mode)) {
		return nil, invalidNativeLog()
	}
	subject := progress.Subject
	if subjectOverride != nil {
		subject = *subjectOverride
	}
	if (a.Subject != "" || a.Phase == nativelogs.ApplyPhase) && a.Subject != subject {
		if subjectOverride != nil || progress.Revision >= a.CaptureRevision {
			return nil, invalidNativeLog()
		}
		return nil, errors.New("native log subject progress is not retained yet")
	}
	if a.Subject == "" && a.Phase == nativelogs.PlanPhase && a.Inherited {
		return nil, invalidNativeLog()
	}
	return command, nil
}

func (s *Server) loadNativeLog(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, descriptor nativelogs.Descriptor) ([]byte, error) {
	command, err := nativeLogCommand(ctx, tx, dep, descriptor, nil)
	if err != nil {
		return nil, err
	}
	if command.Request.EncryptedLogsRecipient == "" {
		return nil, invalidNativeLog()
	}
	material, err := model.GetDeploymentLogMaterial(ctx, tx, dep.OrgId, dep.Id)
	if err != nil {
		return nil, err
	}
	if material.Recipient != command.Request.EncryptedLogsRecipient {
		return nil, invalidNativeLog()
	}
	if s.RunnerLogsReader == nil {
		return nil, nativelogs.ErrArtifact
	}
	reader, err := s.RunnerLogsReader(ctx, descriptor.ObjectKey)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	content, err := io.ReadAll(io.LimitReader(reader, nativelogs.CiphertextLimit+1))
	if err != nil || descriptor.VerifyCiphertext(content) != nil {
		return nil, nativelogs.ErrArtifact
	}
	identity, err := (DeploymentLogMaterialService{Vault: s.Vault}).Identity(ctx, *material)
	if err != nil {
		return nil, err
	}
	envelope, err := nativelogs.Decode(content, identity, descriptor.Authority)
	if err != nil || envelope.Status != descriptor.Status {
		return nil, nativelogs.ErrArtifact
	}
	return envelope.Logs, nil
}

// A ready event accepts only metadata. It never settles or releases execution.
func (s *Server) ApplyNativeLogReady(ctx context.Context, event hmessaging.EventEnvelope) error {
	ctx, cancel := context.WithTimeout(ctx, nativeLogReadTimeout)
	defer cancel()
	raw, err := json.Marshal(event)
	if err != nil {
		return invalidNativeLog()
	}
	_, descriptor, err := nativelogs.ParseReady(raw)
	if err != nil {
		return invalidNativeLog()
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	dep, _, _, _, err := s.Database.GetDeployment(ctx, tx, event.OrganizationID, uuid.MustParse(event.DeploymentID), model.GetModeForUpdate)
	if err != nil {
		return err
	}
	content, err := s.loadNativeLog(ctx, tx, dep, descriptor)
	if err != nil {
		return err
	}
	clear(content)
	if err = model.AddPipelineLogReference(ctx, tx, dep.Id, descriptor); err != nil {
		if _, conflict := model.IsErrConflict(err); conflict {
			return invalidNativeLog()
		}
		return err
	}
	return tx.Commit()
}

func selectNativeLog(progress model.PipelineProgress, phase, execution string, revision *int64) (*nativelogs.Descriptor, string) {
	var selected *nativelogs.Descriptor
	for _, reference := range progress.LogReferences {
		a := reference.Authority
		if a.Phase != phase || execution != "" && a.ExecutionID != execution || revision != nil && a.CaptureRevision != *revision {
			continue
		}
		if selected == nil || a.CaptureRevision > selected.Authority.CaptureRevision {
			copy := reference
			selected = &copy
		}
	}
	if selected != nil {
		return selected, "available"
	}
	if progress.LogReferencesLimited {
		return nil, "reference_limit"
	}
	capture := progress.LogCapture
	if capture != nil && capture.Authority.Phase == phase && (execution == "" || capture.Authority.ExecutionID == execution) && (revision == nil || capture.Authority.CaptureRevision == *revision) {
		return nil, capture.Status
	}
	return nil, "not_captured"
}

func (s *Server) getNativeDeploymentLogs(ctx context.Context, request GetDeploymentLogsRequestObject, dep *model.DeploymentSummary) (GetDeploymentLogsResponseObject, error) {
	ctx, cancel := context.WithTimeout(ctx, nativeLogReadTimeout)
	defer cancel()
	if c := middleware.GetEchoCtx(ctx); c != nil {
		c.Response().Header().Set("Cache-Control", "no-store")
		c.Response().Header().Set("X-Content-Type-Options", "nosniff")
	}
	uid, callerErr := pipelineCaller(ctx)
	if callerErr != nil {
		return nil, callerErr
	}
	if err := s.checkEnvAuthorization(ctx, uid, request.OrgId, dep.DeploymentEnvUuid, PermissionDeploymentRead); err != nil {
		return nil, err
	}
	if err := s.checkEnvAuthorization(ctx, uid, request.OrgId, dep.DeploymentEnvUuid, PermissionDeploymentLogsRead); err != nil {
		return nil, err
	}
	unavailable := func(reason string) (GetDeploymentLogsResponseObject, error) {
		return GetDeploymentLogs409JSONResponse{N409ConflictJSONResponse: Generate409Response("native logs unavailable: " + reason)}, nil
	}
	if request.Params.DecryptKey != nil || !ref.DerefOr(request.Params.Decrypt, false) {
		return unavailable("managed central log access required")
	}
	phase := nativelogs.ApplyPhase
	if model.IsPlanDeployment(dep.Mode) {
		phase = nativelogs.PlanPhase
	}
	if request.Params.Phase != nil {
		phase = string(*request.Params.Phase)
	}
	execution := ref.DerefOr(request.Params.ExecutionId, "")
	if phase != nativelogs.PlanPhase && phase != nativelogs.ApplyPhase ||
		execution != "" && (!nativeLogExecutionID.MatchString(execution) || execution == "." || execution == "..") ||
		request.Params.CaptureRevision != nil && *request.Params.CaptureRevision < 1 {
		return GetDeploymentLogs400JSONResponse{N400BadRequestJSONResponse: Generate400Response("invalid native log selector")}, nil
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	command, progress, _, err := readNativeLogCommand(ctx, tx, dep)
	if err != nil {
		return unavailable("accepted command unavailable")
	}
	if command.Request.EncryptedLogsRecipient == "" {
		return unavailable("log_export_not_managed")
	}
	descriptor, status := selectNativeLog(progress, phase, execution, request.Params.CaptureRevision)
	if descriptor == nil {
		return unavailable(status)
	}
	if phase == nativelogs.PlanPhase && ref.DerefOr(request.Params.Subject, "") != descriptor.Authority.Subject {
		return unavailable("plan subject changed; reload status")
	}
	if phase != nativelogs.PlanPhase && request.Params.Subject != nil {
		return GetDeploymentLogs400JSONResponse{N400BadRequestJSONResponse: Generate400Response("subject requires plan phase")}, nil
	}
	content, err := s.loadNativeLog(ctx, tx, dep, *descriptor)
	if err != nil {
		return unavailable("retained artifact or managed identity unavailable")
	}
	defer clear(content)
	return GetDeploymentLogs200TextResponse{Body: string(content), Headers: GetDeploymentLogs200ResponseHeaders{CacheControl: ref.Ref("no-store"), XContentTypeOptions: ref.Ref("nosniff")}}, nil
}
