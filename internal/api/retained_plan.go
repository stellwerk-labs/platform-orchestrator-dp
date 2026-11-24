package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genevents"
)

const retainedSnapshotLimit = 3 << 20

type RetainedPlanService struct{ Server *Server }
type RetainedPlanRequest struct {
	OrgID, ProjectID, EnvID string
	Environment             cp.Environment
	Manifest                DeploymentManifest
	ModuleVersions          map[string]string
	RestrictedConfirmations []uuid.UUID
	Backend, Tool           string
}

type retainedLifecycle struct {
	CarryForward map[string]uuid.UUID `json:"carry_forward"`
	Confirmed    []uuid.UUID          `json:"confirmed"`
}
type retainedSnapshot struct {
	Binding    model.RetainedPlanBinding `json:"binding"`
	OS         string                    `json:"os"`
	Arch       string                    `json:"arch"`
	ToolSHA256 string                    `json:"tool_sha256"`
	Archive    []byte                    `json:"archive"`
	Review     []byte                    `json:"review"`
}

func hashRetained(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (r *RetainedPlanService) CreatePlan(ctx context.Context, actor uuid.UUID, req RetainedPlanRequest, operationID uuid.UUID) (*model.RetainedPlan, error) {
	s := r.Server
	if !validRetainedPlanRequest(actor, operationID, req) {
		return nil, model.NewErrBadRequest("complete retained plan scope is required")
	}
	if err := s.checkEnvAuthorization(ctx, actor, req.OrgID, req.Environment.Uuid, PermissionDeploymentWrite); err != nil {
		return nil, err
	}
	requestBytes, err := json.Marshal(struct {
		Manifest              DeploymentManifest
		ModuleVersions        map[string]string
		Confirmations         []uuid.UUID
		Backend, Tool, Runner string
	}{req.Manifest, req.ModuleVersions, req.RestrictedConfirmations, req.Backend, req.Tool, *req.Environment.RunnerId})
	if err != nil {
		return nil, err
	}
	requestDigest := hashRetained(requestBytes)
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	existing, err := findRetainedPlanReplay(ctx, tx, actor, req, operationID, requestDigest)
	if err != nil || existing != nil {
		return existing, err
	}
	runner, err := s.requestedPipelineRunner(ctx, req.OrgID, &req.Environment)
	if err != nil {
		return nil, err
	}
	if kind, _ := runner.RunnerConfiguration.Discriminator(); kind == string(cp.RunnerTypeNativeAgent) {
		return nil, model.NewErrConflict("Native plans require the deployment pipeline Plan API")
	}
	if err := model.RequireStateCustody(ctx, tx, req.OrgID, req.ProjectID, req.EnvID, req.Environment.Uuid, *req.Environment.RunnerId, nil); err != nil {
		return nil, err
	}
	material, err := (DeploymentLogMaterialService{Vault: s.Vault}).Generate(ctx)
	if err != nil {
		return nil, err
	}
	request := deploymentRequest{Manifest: &req.Manifest, ModuleVersions: req.ModuleVersions, RestrictedConfirmations: req.RestrictedConfirmations, EncryptedOutputsRecipient: &material.Recipient, ManagedLogs: true}
	options := s.deploymentOptions(ctx, actor, req.OrgID, req.Environment, model.DeploymentModeDeployPlan, request, "", false)
	options.RequireExistingRunner = true
	var capture DeploymentPlanCapture
	options.CapturePlan = &capture
	deployment, messages, _, err := CreateDeploymentWithOptions(ctx, s.Logger, actor, req.OrgID, req.ProjectID, req.EnvID, req.Environment, model.DeploymentModeDeployPlan, opt.Empty[uuid.UUID](), req.Manifest, &material.Recipient, nil, nil, false, nil, s.ControlPlaneClient, s.Database, tx, options)
	if err != nil {
		return nil, err
	}
	lifecycle, err := json.Marshal(retainedLifecycle{capture.CarryForwardModuleVersions, capture.ConfirmedRestrictedVersionUUIDs})
	if err != nil {
		return nil, err
	}
	p := newRetainedPlan(req, actor, operationID, requestDigest, deployment, capture, *material, lifecycle)
	if err = model.InsertRetainedPlan(ctx, tx, p); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	reliableoutbox.OptimisticPublish(ctx, s.Logger, s.Database.AsReliableOutboxStore(), s.EventPublisher, messages)
	return r.GetPlan(ctx, req.OrgID, operationID)
}

func newRetainedPlan(req RetainedPlanRequest, actor, operationID uuid.UUID, requestDigest string, deployment *model.DeploymentSummary, capture DeploymentPlanCapture, material model.DeploymentLogMaterial, lifecycle []byte) model.RetainedPlan {
	binding := model.RetainedPlanBinding{
		Schema:             "stellwerk.retained-plan/v1",
		OrgID:              req.OrgID,
		EnvironmentID:      req.Environment.Uuid.String(),
		RunnerID:           deployment.RunnerId,
		SourceDeploymentID: deployment.Id.String(),
		BaseDeploymentID:   capture.BaseDeploymentID.String(),
		Backend:            req.Backend,
		Tool:               req.Tool,
	}
	p := model.RetainedPlan{
		ID:                 operationID,
		OrgID:              req.OrgID,
		ProjectID:          req.ProjectID,
		EnvID:              req.EnvID,
		EnvironmentID:      req.Environment.Uuid,
		CreatorID:          actor,
		SourceDeploymentID: deployment.Id,
		BaseDeploymentID:   capture.BaseDeploymentID,
		RunnerID:           deployment.RunnerId,
		Binding:            binding,
		Recipient:          material.Recipient,
		EncryptedIdentity:  material.EncryptedIdentity,
		Lifecycle:          lifecycle,
		RequestDigest:      requestDigest,
	}
	return p
}

func validRetainedPlanRequest(actor, operationID uuid.UUID, req RetainedPlanRequest) bool {
	return actor != uuid.Nil && operationID != uuid.Nil && req.OrgID != "" && req.Environment.Uuid != uuid.Nil && req.Environment.RunnerId != nil && req.Backend != "" && req.Tool != ""
}

func findRetainedPlanReplay(ctx context.Context, tx model.Tx, actor uuid.UUID, req RetainedPlanRequest, id uuid.UUID, digest string) (*model.RetainedPlan, error) {
	existing, err := model.ReadRetainedPlan(ctx, tx, req.OrgID, id, true)
	if _, missing := model.IsErrNotFound(err); missing {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if existing.CreatorID != actor || existing.ProjectID != req.ProjectID || existing.EnvID != req.EnvID || existing.EnvironmentID != req.Environment.Uuid || existing.RequestDigest != digest {
		return nil, model.NewErrConflict("retained operation identity mismatch")
	}
	return existing, nil
}

func (r *RetainedPlanService) GetPlan(ctx context.Context, org string, id uuid.UUID) (*model.RetainedPlan, error) {
	tx, err := r.Server.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	return model.ReadRetainedPlan(ctx, tx, org, id, false)
}

func decryptRetained(ctx context.Context, s *Server, p *model.RetainedPlan, ciphertext []byte) (*retainedSnapshot, error) {
	identity, err := (DeploymentLogMaterialService{Vault: s.Vault}).Identity(ctx, model.DeploymentLogMaterial{Recipient: p.Recipient, EncryptedIdentity: p.EncryptedIdentity})
	if err != nil {
		return nil, err
	}
	rd, err := age.Decrypt(bytes.NewReader(ciphertext), identity)
	if err != nil {
		return nil, errors.New("invalid retained material")
	}
	raw, err := io.ReadAll(io.LimitReader(rd, retainedSnapshotLimit+1))
	if err != nil || len(raw) > retainedSnapshotLimit {
		return nil, errors.New("retained material exceeds limit")
	}
	defer clear(raw)
	var snapshot retainedSnapshot
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&snapshot); err != nil || !validRetainedSnapshot(snapshot, p.Binding) {
		return nil, errors.New("invalid retained material")
	}
	if dec.Decode(new(any)) != io.EOF {
		return nil, errors.New("invalid retained material")
	}
	return &snapshot, nil
}

func validRetainedSnapshot(snapshot retainedSnapshot, binding model.RetainedPlanBinding) bool {
	return snapshot.Binding == binding && snapshot.OS != "" && snapshot.Arch != "" && len(snapshot.ToolSHA256) == 64 && len(snapshot.Archive) > 0 && json.Valid(snapshot.Review)
}

func (r *RetainedPlanService) Review(ctx context.Context, org string, id uuid.UUID) ([]byte, error) {
	p, err := r.GetPlan(ctx, org, id)
	if err != nil {
		return nil, err
	}
	if len(p.Ciphertext) == 0 || !p.CiphertextSHA256.Valid || hashRetained(p.Ciphertext) != p.CiphertextSHA256.String {
		return nil, model.NewErrConflict("retained plan not ready")
	}
	snapshot, err := decryptRetained(ctx, r.Server, p, p.Ciphertext)
	if err != nil {
		return nil, err
	}
	return snapshot.Review, nil
}

func retainedArtifactSubject(p *model.RetainedPlan, dep *model.DeploymentSummary, manifest model.EncodedDeploymentManifest, graph model.EncodedDeploymentGraph, tofu model.RawTofu, ciphertext []byte) (string, error) {
	artifacts, err := json.Marshal(dep.ModuleArtifacts)
	if err != nil {
		return "", err
	}
	input, err := json.Marshal(struct {
		Binding                                      model.RetainedPlanBinding `json:"binding"`
		Manifest, Graph, Tofu, Artifacts, Ciphertext string
	}{p.Binding, hashRetained(manifest), hashRetained(graph), hashRetained(tofu), hashRetained(artifacts), hashRetained(ciphertext)})
	if err != nil {
		return "", err
	}
	return hashRetained(input), nil
}

// ingestRetainedResult runs in the same transaction as the authoritative result.
func ingestRetainedResult(ctx context.Context, s *Server, tx model.Tx, dep *model.DeploymentSummary, body *DeploymentResultsUpdateBody) error {
	var id uuid.UUID
	err := tx.QueryRowContext(ctx, `SELECT id FROM retained_plans WHERE org_id=$1 AND source_deployment_id=$2 FOR UPDATE`, dep.OrgId, dep.Id).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if body.Status != Success {
		return nil
	}
	ciphertext, err := decodeRetainedOutput(body.Outputs)
	if err != nil {
		return err
	}
	p, err := model.ReadRetainedPlan(ctx, tx, dep.OrgId, id, false)
	if err != nil {
		return err
	}
	snapshot, err := decryptRetained(ctx, s, p, ciphertext)
	if err != nil {
		return err
	}
	clear(snapshot.Archive)
	clear(snapshot.Review)
	digest := hashRetained(ciphertext)
	_, manifest, tofu, graph, err := s.Database.GetDeployment(ctx, tx, dep.OrgId, dep.Id, model.GetModeDefault)
	if err != nil {
		return err
	}
	subject, err := retainedArtifactSubject(p, dep, manifest, graph, tofu, ciphertext)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE retained_plans SET ciphertext=$2,ciphertext_sha256=$3,subject=$4 WHERE id=$1 AND ciphertext IS NULL`, id, ciphertext, digest, subject)
	return err
}

func decodeRetainedOutput(encoded *string) ([]byte, error) {
	if encoded == nil || len(*encoded) > 4<<20 {
		return nil, errors.New("retained plan result unavailable")
	}
	ciphertext, err := base64.StdEncoding.DecodeString(*encoded)
	if err != nil || len(ciphertext) == 0 || len(ciphertext) > retainedSnapshotLimit {
		return nil, errors.New("retained plan result unavailable")
	}
	return ciphertext, nil
}

func (r *RetainedPlanService) HandleKey(c echo.Context) error {
	ctx := c.Request().Context()
	org, runner := c.Param("orgId"), c.Param("runnerId")
	depID, err := uuid.Parse(c.Param("deploymentId"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest)
	}
	token := c.Request().Header.Get("X-Deployment-Token")
	if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(util.GenerateHashedRunnerToken(r.Server.RunnerTokenSalt, org, depID.String()))) != 1 {
		return echo.NewHTTPError(http.StatusUnauthorized)
	}
	tx, err := r.Server.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var id uuid.UUID
	err = tx.QueryRowContext(ctx,
		`SELECT p.id FROM retained_plans p JOIN deployments d ON d.id=p.apply_deployment_id WHERE p.org_id=$1 AND p.runner_id=$2 AND p.apply_deployment_id=$3 AND d.runner_id=$2 AND d.status='executing' AND d.native_target IS NULL AND p.ciphertext IS NOT NULL`,
		org, runner, depID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusNotFound)
	}
	if err != nil {
		return err
	}
	p, err := model.ReadRetainedPlan(ctx, tx, org, id, false)
	if err != nil {
		return err
	}
	identity, err := (DeploymentLogMaterialService{Vault: r.Server.Vault}).Identity(ctx, model.DeploymentLogMaterial{Recipient: p.Recipient, EncryptedIdentity: p.EncryptedIdentity})
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable)
	}
	key, ok := identity.(*age.X25519Identity)
	if !ok {
		return echo.NewHTTPError(http.StatusServiceUnavailable)
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, map[string]string{"identity": key.String()})
}

func (r *RetainedPlanService) ApplyPlan(ctx context.Context, actor uuid.UUID, org string, id uuid.UUID, subject, key string) (_ *model.DeploymentSummary, resultErr error) {
	s := r.Server
	if !s.validRetainedApplyRequest(actor, org, id, subject, key) {
		return nil, model.NewErrBadRequest("complete retained apply authority is required")
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	p, err := model.ReadRetainedPlan(ctx, tx, org, id, true)
	if err != nil {
		return nil, err
	}
	if err = s.checkEnvAuthorization(ctx, actor, org, p.EnvironmentID, PermissionDeploymentWrite); err != nil {
		return nil, err
	}
	keyDigest := hashRetained([]byte(key))
	if p.ApplyDeploymentID.Valid {
		return s.recoverRetainedApply(ctx, tx, p, actor, subject, keyDigest)
	}
	runner, runnerErr := s.ControlPlaneClient.GetRunnerWithResponse(ctx, org, p.RunnerID)
	if runnerErr != nil || runner.JSON200 == nil {
		return nil, model.NewErrConflict("Runner unavailable")
	}
	if kind, _ := runner.JSON200.RunnerConfiguration.Discriminator(); kind == "native-agent" {
		return nil, model.NewErrBadRequest("native-agent does not support the legacy retained apply protocol")
	}
	if native, e := model.RunnerSupportsPipeline(runner.JSON200.RunnerConfiguration); e != nil {
		return nil, e
	} else if native {
		return nil, model.NewErrBadRequest("pipeline Runner does not support the legacy retained apply protocol")
	}
	source, err := s.validateRetainedApply(ctx, tx, p)
	if err != nil {
		return nil, err
	}
	var lifecycle retainedLifecycle
	if err = json.Unmarshal(p.Lifecycle, &lifecycle); err != nil {
		return nil, err
	}
	if err := model.RequireStateCustody(ctx, tx, org, p.ProjectID, p.EnvID, p.EnvironmentID, p.RunnerID, nil); err != nil {
		return nil, err
	}
	admission := s.DeploymentAdmissions.NewAttempt(org, source.ProjectUUID, p.EnvironmentID, actor, model.DeploymentModeDeploy, key)
	defer func() {
		if resultErr != nil {
			_ = tx.Rollback()
			resultErr = sealRetainedAdmission(ctx, admission, resultErr)
		}
	}()
	compiled := ordinaryDeploymentCompiledIntent{
		ProjectID:                       p.ProjectID,
		EnvironmentID:                   p.EnvID,
		RunnerID:                        p.RunnerID,
		EffectiveBaseDeploymentID:       p.BaseDeploymentID,
		Manifest:                        source.Manifest,
		Graph:                           source.Graph,
		Tofu:                            source.Tofu,
		RunnerLogLevel:                  source.Deployment.RunnerLogLevel,
		EncryptedOutputsRecipient:       p.Recipient,
		ManagedLogs:                     true,
		CarryForwardModuleVersions:      lifecycle.CarryForward,
		ConfirmedRestrictedVersionUUIDs: lifecycle.Confirmed,
		RetainedSubject:                 hashRetained([]byte(p.Subject.String + ":" + subject)),
	}
	if err = admission.Authorize(ctx, tx, source.Deployment.ModuleArtifacts, compiled); err != nil {
		return nil, err
	}
	dep, messages, err := s.createRetainedApply(ctx, tx, p, source, admission, actor, subject, keyDigest)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	reliableoutbox.OptimisticPublish(ctx, s.Logger, s.Database.AsReliableOutboxStore(), s.EventPublisher, messages)
	return dep, nil
}

func (s *Server) validRetainedApplyRequest(actor uuid.UUID, org string, id uuid.UUID, subject, key string) bool {
	return actor != uuid.Nil && org != "" && id != uuid.Nil && subject != "" && key != "" && s.DeploymentAdmissions != nil
}

func sealRetainedAdmission(ctx context.Context, admission *OrdinaryDeploymentAdmissionAttempt, original error) error {
	if err := admission.SealAbsent(ctx); err != nil {
		return fmt.Errorf("failed to seal retained admission: %w", err)
	}
	return original
}

// createRetainedApply adds the admitted effect and its outbox entry to the caller's transaction.
func (s *Server) createRetainedApply(ctx context.Context, tx model.Tx, p *model.RetainedPlan, source *retainedApplySource, admission *OrdinaryDeploymentAdmissionAttempt, actor uuid.UUID, subject, keyDigest string) (*model.DeploymentSummary, []*hstandardoutbox.PendingEventMessage, error) {
	logMaterial, err := (DeploymentLogMaterialService{Vault: s.Vault}).Generate(ctx)
	if err != nil {
		return nil, nil, err
	}
	// A first-ever plan is the DP's synthetic state head even though its
	// underlying backend base is empty. Compare that exact sentinel instead
	// of treating it as an intervening state-changing deployment.
	expectedHead := p.BaseDeploymentID
	if expectedHead == uuid.Nil {
		expectedHead = p.SourceDeploymentID
	}
	dep, err := s.Database.CreateDeployment(
		ctx,
		tx,
		p.OrgID,
		p.ProjectID,
		p.EnvID,
		model.CreateDeploymentParams{
			CreatedBy:                 actor,
			DeploymentEnvUuid:         p.EnvironmentID,
			Mode:                      model.DeploymentModeDeploy,
			Manifest:                  source.Manifest,
			Graph:                     source.Graph,
			Tofu:                      source.Tofu,
			RunnerId:                  p.RunnerID,
			EncryptedOutputsRecipient: opt.Of(p.Recipient),
			EncryptedLogsRecipient:    opt.Of(logMaterial.Recipient),
			Metrics:                   source.Deployment.Metrics,
			RunnerLogLevel:            source.Deployment.RunnerLogLevel,
			ModuleArtifacts:           source.Deployment.ModuleArtifacts,
			ExpectedBaseDeploymentID:  opt.Of(expectedHead),
			IdempotencyKeyDigest:      opt.Of(keyDigest),
		},
	)
	if err != nil {
		return nil, nil, err
	}
	if err := model.StoreDeploymentLogMaterial(ctx, tx, p.OrgID, dep.Id, *logMaterial); err != nil {
		return nil, nil, err
	}
	parsed, err := graphs.FromJson(bytes.NewReader(source.Graph))
	if err != nil {
		return nil, nil, err
	}
	if err = s.Database.InitActiveResourcesFromGraph(ctx, tx, p.EnvironmentID, dep.Id, parsed); err != nil {
		return nil, nil, err
	}
	if err = admission.Bind(ctx, tx, dep.Id); err != nil {
		return nil, nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE retained_plans SET apply_deployment_id=$2,apply_key_digest=$3,approval_subject=$4 WHERE id=$1 AND apply_deployment_id IS NULL`, p.ID, dep.Id, keyDigest, subject); err != nil {
		return nil, nil, err
	}
	messages, err := s.Database.InsertPendingEventMessages(ctx, tx, []*hstandardoutbox.PendingEventMessage{{Subject: string(genevents.IoPlatformOrchestratorDeploymentCreated), Payload: model.ConvertDeploymentToEventPayload(dep)}})
	if err != nil {
		return nil, nil, err
	}
	return dep, messages, nil
}

// recoverRetainedApply returns only the effect already bound to this identity.
func (s *Server) recoverRetainedApply(ctx context.Context, tx model.Tx, p *model.RetainedPlan, actor uuid.UUID, subject, keyDigest string) (*model.DeploymentSummary, error) {
	if !p.ApplyKeyDigest.Valid || p.ApplyKeyDigest.String != keyDigest || !p.ApprovalSubject.Valid || p.ApprovalSubject.String != subject {
		return nil, model.NewErrConflict("retained apply identity mismatch")
	}
	d, _, _, _, err := s.Database.GetDeployment(ctx, tx, p.OrgID, p.ApplyDeploymentID.UUID, model.GetModeDefault)
	if err == nil && d.CreatedBy != actor {
		return nil, model.NewErrConflict("retained apply actor mismatch")
	}
	return d, err
}

type retainedApplySource struct {
	Deployment  *model.DeploymentSummary
	Manifest    model.EncodedDeploymentManifest
	Tofu        model.RawTofu
	Graph       model.EncodedDeploymentGraph
	ProjectUUID uuid.UUID
}

func (s *Server) validateRetainedApply(ctx context.Context, tx model.Tx, p *model.RetainedPlan) (*retainedApplySource, error) {
	if !retainedMaterialReady(p) {
		return nil, model.NewErrConflict("retained plan unavailable or expired")
	}
	dep, manifest, tofu, graph, err := s.Database.GetDeployment(ctx, tx, p.OrgID, p.SourceDeploymentID, model.GetModeDefault)
	if err != nil {
		return nil, err
	}
	actual, err := retainedArtifactSubject(p, dep, manifest, graph, tofu, p.Ciphertext)
	if err != nil || actual != p.Subject.String {
		return nil, model.NewErrConflict("retained source material changed")
	}
	if dep.Status != model.DeploymentStatusSucceeded || dep.DeploymentEnvUuid != p.EnvironmentID || dep.RunnerId != p.RunnerID {
		return nil, model.NewErrConflict("retained source is not successful")
	}
	env, err := s.retainedTarget(ctx, p.OrgID, p.ProjectID, p.EnvID, p.EnvironmentID, p.RunnerID)
	if err != nil {
		return nil, err
	}
	if env.ProjectUuid == nil {
		return nil, model.NewErrConflict("retained execution target changed")
	}
	return &retainedApplySource{Deployment: dep, Manifest: manifest, Tofu: tofu, Graph: graph, ProjectUUID: *env.ProjectUuid}, nil
}

func retainedMaterialReady(p *model.RetainedPlan) bool {
	return !time.Now().After(p.ExpiresAt) && len(p.Ciphertext) > 0 && p.CiphertextSHA256.Valid && p.Subject.Valid && hashRetained(p.Ciphertext) == p.CiphertextSHA256.String
}

func (s *Server) retainedTarget(ctx context.Context, org, project, environment string, environmentID uuid.UUID, runner string) (*cp.Environment, error) {
	resp, err := s.ControlPlaneClient.GetEnvironmentWithResponse(ctx, org, project, environment)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil || resp.JSON200.Uuid != environmentID || resp.JSON200.RunnerId == nil || *resp.JSON200.RunnerId != runner || resp.JSON200.Status == cp.EnvironmentStatusDeleting {
		return nil, model.NewErrConflict("retained execution target changed")
	}
	return resp.JSON200, nil
}

// AuthorizeRetainedDispatch revalidates both human actors and target just before dispatch.
// Core's admission and module lifecycle checks occurred at admission, not here.
func (s *Server) AuthorizeRetainedDispatch(ctx context.Context, dep *model.DeploymentSummary) error {
	if dep.RetainedPlan == nil || dep.RetainedPlan.Export || dep.RetainedPlan.RequesterID == uuid.Nil || dep.CreatedBy == uuid.Nil {
		return errors.New("retained dispatch identity missing")
	}
	if err := s.checkEnvAuthorization(ctx, dep.RetainedPlan.RequesterID, dep.OrgId, dep.DeploymentEnvUuid, PermissionDeploymentWrite); err != nil {
		return err
	}
	if err := s.checkEnvAuthorization(ctx, dep.CreatedBy, dep.OrgId, dep.DeploymentEnvUuid, PermissionDeploymentWrite); err != nil {
		return err
	}
	_, err := s.retainedTarget(ctx, dep.OrgId, dep.ProjectId, dep.EnvId, dep.DeploymentEnvUuid, dep.RunnerId)
	return err
}

// ingestRetainedOutputs preserves the Runner's ciphertext in the same
// transaction as apply settlement. Missing or malformed outputs never change
// a successful infrastructure effect into a failed deployment.
func ingestRetainedOutputs(ctx context.Context, tx model.Tx, dep *model.DeploymentSummary, body *DeploymentResultsUpdateBody) error {
	if body.Status != Success || body.Outputs == nil || len(*body.Outputs) > 4<<20 {
		return nil
	}
	ciphertext, err := base64.StdEncoding.DecodeString(*body.Outputs)
	if err != nil || len(ciphertext) == 0 || len(ciphertext) > retainedSnapshotLimit {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE retained_plans SET apply_outputs=$3 WHERE org_id=$1 AND apply_deployment_id=$2 AND recipient=$4 AND apply_outputs IS NULL`, dep.OrgId, dep.Id, ciphertext, dep.EncryptedOutputsRecipient.Or(""))
	return err
}

// BoundOutputs returns private apply output JSON for an authorized post task.
// Callers must enforce pipeline scope and keep the result out of history.
func (r *RetainedPlanService) BoundOutputs(ctx context.Context, org string, id uuid.UUID) ([]byte, error) {
	p, err := r.GetPlan(ctx, org, id)
	if err != nil {
		return nil, err
	}
	if !p.ApplyDeploymentID.Valid || len(p.ApplyOutputs) == 0 {
		return nil, model.NewErrConflict("retained apply outputs unavailable")
	}
	identity, err := (DeploymentLogMaterialService{Vault: r.Server.Vault}).Identity(ctx, model.DeploymentLogMaterial{Recipient: p.Recipient, EncryptedIdentity: p.EncryptedIdentity})
	if err != nil {
		return nil, err
	}
	reader, err := age.Decrypt(bytes.NewReader(p.ApplyOutputs), identity)
	if err != nil {
		return nil, errors.New("invalid retained apply outputs")
	}
	data, err := io.ReadAll(io.LimitReader(reader, retainedSnapshotLimit+1))
	if err != nil || len(data) > retainedSnapshotLimit || !json.Valid(data) {
		return nil, errors.New("invalid retained apply outputs")
	}
	return data, nil
}
