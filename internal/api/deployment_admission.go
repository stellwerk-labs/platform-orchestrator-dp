package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/genclient"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

var (
	ErrDeploymentAdmissionDenied      = errors.New("core denied Deployment admission")
	ErrDeploymentAdmissionUnavailable = errors.New("core Deployment admission is unavailable")
)

type deploymentAdmissionDeniedError struct {
	statusCode int
}

func (e *deploymentAdmissionDeniedError) Error() string { return ErrDeploymentAdmissionDenied.Error() }
func (e *deploymentAdmissionDeniedError) Unwrap() error { return ErrDeploymentAdmissionDenied }

const (
	DeploymentAdmissionTimeout       = 5 * time.Second
	DeploymentAdmissionRecoveryGrace = time.Minute
	deploymentAdmissionUUIDVersion   = 5
)

var deploymentAdmissionNamespace = uuid.MustParse("91d168c8-a355-4aa7-b071-b9563cfbca3f")

type deploymentAdmissionCore interface {
	AuthorizeDeploymentAdmissionWithResponse(context.Context, string, uuid.UUID, platformorchestratorcp.AuthorizeDeploymentAdmissionJSONRequestBody, ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.AuthorizeDeploymentAdmissionResponse, error)
	SealDeploymentAdmissionAbsenceWithResponse(context.Context, string, uuid.UUID, platformorchestratorcp.SealDeploymentAdmissionAbsenceJSONRequestBody, ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.SealDeploymentAdmissionAbsenceResponse, error)
	SettleDeploymentAdmissionWithResponse(context.Context, string, uuid.UUID, platformorchestratorcp.SettleDeploymentAdmissionJSONRequestBody, ...platformorchestratorcp.RequestEditorFn) (*platformorchestratorcp.SettleDeploymentAdmissionResponse, error)
}

type OrdinaryDeploymentAdmissionService struct {
	Database    model.Databaser
	Core        deploymentAdmissionCore
	BearerToken string
}

type OrdinaryDeploymentAdmissionAttempt struct {
	service                         *OrdinaryDeploymentAdmissionService
	AdmissionID                     uuid.UUID
	OrgID                           string
	ProjectUUID                     uuid.UUID
	EnvironmentUUID                 uuid.UUID
	ActorID                         uuid.UUID
	Mode                            model.DeploymentMode
	requestDigest                   string
	moduleVersions                  map[string]uuid.UUID
	carryForwardModuleVersions      map[string]uuid.UUID
	confirmedRestrictedVersionUUIDs []uuid.UUID
	coreEpoch                       int64
	prepared                        bool
	authorized                      bool
}

type ordinaryDeploymentCompiledIntent struct {
	ProjectID                       string
	EnvironmentID                   string
	RunnerID                        string
	EffectiveBaseDeploymentID       uuid.UUID
	RollbackToDeploymentID          uuid.NullUUID
	Manifest                        []byte
	Graph                           []byte
	Tofu                            []byte
	EncryptedOutputsRecipient       string
	EncryptedLogsRecipient          string
	RunnerLogLevel                  string
	CarryForwardModuleVersions      map[string]uuid.UUID
	ConfirmedRestrictedVersionUUIDs []uuid.UUID
}

func (s *OrdinaryDeploymentAdmissionService) NewAttempt(orgID string, projectUUID, environmentUUID, actorID uuid.UUID, mode model.DeploymentMode, requestKey string) *OrdinaryDeploymentAdmissionAttempt {
	if s == nil {
		return nil
	}
	// An omitted caller key still needs one durable attempt identity before any
	// remote effect. Random v4 IDs cannot collide with the v5 caller-key namespace.
	// Recovery uses the persisted receipt, never a newly generated attempt.
	var admissionID uuid.UUID
	if requestKey == "" {
		admissionID = uuid.New()
	} else {
		identity, _ := json.Marshal([]string{"ordinary-deployment-admission", orgID, projectUUID.String(), environmentUUID.String(), requestKey})
		admissionID = uuid.NewHash(sha256.New(), deploymentAdmissionNamespace, identity, deploymentAdmissionUUIDVersion)
	}
	return &OrdinaryDeploymentAdmissionAttempt{
		service: s, AdmissionID: admissionID,
		OrgID: orgID, ProjectUUID: projectUUID, EnvironmentUUID: environmentUUID, ActorID: actorID, Mode: mode,
	}
}

func (a *OrdinaryDeploymentAdmissionAttempt) Authorize(ctx context.Context, tx model.Tx, artifacts []model.ModuleArtifactRequirement, compiled ordinaryDeploymentCompiledIntent) error {
	if a == nil {
		return nil
	}
	if a.service == nil || a.service.Core == nil || a.service.Database == nil || a.service.BearerToken == "" {
		return ErrDeploymentAdmissionUnavailable
	}
	if a.AdmissionID == uuid.Nil || a.OrgID == "" || a.ProjectUUID == uuid.Nil || a.EnvironmentUUID == uuid.Nil ||
		a.ActorID == uuid.Nil {
		return model.NewErrBadRequest("complete Deployment admission identity is required")
	}
	a.moduleVersions = make(map[string]uuid.UUID, len(artifacts))
	for _, artifact := range artifacts {
		if artifact.ModuleUUID == uuid.Nil || artifact.VersionUUID == uuid.Nil {
			return model.NewErrConflict("Deployment admission requires immutable Module and Version UUIDs")
		}
		key := artifact.ModuleUUID.String()
		if existing, found := a.moduleVersions[key]; found && existing != artifact.VersionUUID {
			return model.NewErrConflict("Deployment resolves one Module UUID to multiple Versions")
		}
		a.moduleVersions[key] = artifact.VersionUUID
	}
	if a.Mode == model.DeploymentModeDestroy {
		a.moduleVersions = map[string]uuid.UUID{}
	}
	a.carryForwardModuleVersions = compiled.CarryForwardModuleVersions
	a.confirmedRestrictedVersionUUIDs = compiled.ConfirmedRestrictedVersionUUIDs
	manifestDigest := sha256.Sum256(compiled.Manifest)
	graphDigest := sha256.Sum256(compiled.Graph)
	tofuDigest := sha256.Sum256(compiled.Tofu)
	artifactsJSON, err := json.Marshal(artifacts)
	if err != nil {
		return err
	}
	artifactsDigest := sha256.Sum256(artifactsJSON)
	encoded, err := json.Marshal(struct {
		AdmissionID                     uuid.UUID            `json:"admission_id"`
		OrgID                           string               `json:"org_id"`
		ProjectUUID                     uuid.UUID            `json:"project_uuid"`
		EnvironmentUUID                 uuid.UUID            `json:"environment_uuid"`
		ActorID                         uuid.UUID            `json:"actor_id"`
		Mode                            model.DeploymentMode `json:"mode"`
		ModuleVersions                  map[string]uuid.UUID `json:"module_versions"`
		CarryForwardModuleVersions      map[string]uuid.UUID `json:"carry_forward_module_versions"`
		ConfirmedRestrictedVersionUUIDs []uuid.UUID          `json:"confirmed_restricted_version_uuids"`
		ProjectID                       string               `json:"project_id"`
		EnvironmentID                   string               `json:"environment_id"`
		RunnerID                        string               `json:"runner_id"`
		EffectiveBase                   uuid.UUID            `json:"effective_base_deployment_id"`
		RollbackTo                      uuid.NullUUID        `json:"rollback_to_deployment_id"`
		ManifestSHA256                  string               `json:"manifest_sha256"`
		GraphSHA256                     string               `json:"graph_sha256"`
		TofuSHA256                      string               `json:"tofu_sha256"`
		ArtifactsSHA256                 string               `json:"module_artifacts_sha256"`
		OutputsRecipient                string               `json:"encrypted_outputs_recipient"`
		LogsRecipient                   string               `json:"encrypted_logs_recipient"`
		RunnerLogLevel                  string               `json:"runner_log_level"`
	}{
		a.AdmissionID, a.OrgID, a.ProjectUUID, a.EnvironmentUUID, a.ActorID, a.Mode, a.moduleVersions,
		a.carryForwardModuleVersions, a.confirmedRestrictedVersionUUIDs,
		compiled.ProjectID, compiled.EnvironmentID, compiled.RunnerID, compiled.EffectiveBaseDeploymentID,
		compiled.RollbackToDeploymentID, hex.EncodeToString(manifestDigest[:]), hex.EncodeToString(graphDigest[:]),
		hex.EncodeToString(tofuDigest[:]), hex.EncodeToString(artifactsDigest[:]), compiled.EncryptedOutputsRecipient,
		compiled.EncryptedLogsRecipient, compiled.RunnerLogLevel,
	})
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	a.requestDigest = hex.EncodeToString(digest[:])
	if err := a.prepare(ctx); err != nil {
		return err
	}
	a.prepared = true
	receipt, err := model.GetDeploymentAdmissionReceipt(ctx, tx, a.OrgID, a.AdmissionID)
	if err != nil {
		return err
	}
	if receipt.Disposition != "pending" {
		return model.NewErrConflict("Deployment admission identifier already has a durable outcome")
	}
	return a.authorizeCore(ctx)
}

func (a *OrdinaryDeploymentAdmissionAttempt) prepare(ctx context.Context) error {
	tx, err := a.service.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := model.PrepareDeploymentAdmissionReceipt(ctx, tx, a.receipt("pending", uuid.Nil, "")); err != nil {
		return err
	}
	return tx.Commit()
}

func (a *OrdinaryDeploymentAdmissionAttempt) authorizeCore(ctx context.Context) error {
	intent := a.receipt("pending", uuid.Nil, "")
	response, err := a.service.Core.AuthorizeDeploymentAdmissionWithResponse(ctx, a.OrgID, a.AdmissionID,
		deploymentAdmissionRequest(&intent), a.service.bearer())
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDeploymentAdmissionUnavailable, err)
	}
	if response.StatusCode() != http.StatusOK || response.JSON200 == nil {
		if response.StatusCode() == http.StatusBadRequest || response.StatusCode() == http.StatusForbidden || response.StatusCode() == http.StatusNotFound || response.StatusCode() == http.StatusConflict {
			return &deploymentAdmissionDeniedError{statusCode: response.StatusCode()}
		}
		return fmt.Errorf("%w: Core returned status %d", ErrDeploymentAdmissionUnavailable, response.StatusCode())
	}
	receipt := response.JSON200
	if receipt.AdmissionId != a.AdmissionID || receipt.ProjectUuid != a.ProjectUUID || receipt.EnvironmentUuid != a.EnvironmentUUID ||
		receipt.RequestDigest != a.requestDigest || receipt.Mode != string(a.Mode) || receipt.State != platformorchestratorcp.AdmissionStateActive || receipt.Epoch <= 0 || receipt.Outcome != nil {
		return fmt.Errorf("%w: Core returned a mismatched receipt", ErrDeploymentAdmissionUnavailable)
	}
	a.coreEpoch = receipt.Epoch
	a.authorized = true
	return nil
}

func (a *OrdinaryDeploymentAdmissionAttempt) Bind(ctx context.Context, tx model.Tx, deploymentID uuid.UUID) error {
	if a == nil {
		return nil
	}
	if !a.authorized || deploymentID == uuid.Nil {
		return errors.New("Deployment admission must be authorized before binding")
	}
	_, err := model.BindDeploymentAdmissionReceipt(ctx, tx, a.receipt("pending", uuid.Nil, ""), deploymentID, a.coreEpoch)
	return err
}

func (a *OrdinaryDeploymentAdmissionAttempt) SealAbsent(ctx context.Context) error {
	if a == nil || !a.prepared {
		return nil
	}
	return a.service.sealAbsent(ctx, a.receipt("pending", uuid.Nil, ""))
}

func (s *OrdinaryDeploymentAdmissionService) sealAbsent(ctx context.Context, intent model.DeploymentAdmissionReceipt) error {
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := model.SealDeploymentAdmissionAbsent(ctx, tx, intent, intent.CoreEpoch)
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	response, err := s.Core.SealDeploymentAdmissionAbsenceWithResponse(
		ctx, receipt.OrgID, receipt.AdmissionID, deploymentAdmissionRequest(receipt), s.bearer(),
	)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDeploymentAdmissionUnavailable, err)
	}
	if response.StatusCode() != http.StatusOK || response.JSON200 == nil ||
		!matchingAdmissionReceipt(response.JSON200, receipt, platformorchestratorcp.AdmissionStateSettled, string(platformorchestratorcp.AdmissionOutcomeAbsent)) {
		return fmt.Errorf("%w: Core rejected or mismatched absence receipt", ErrDeploymentAdmissionUnavailable)
	}
	return s.markCoreSettled(ctx, receipt, string(platformorchestratorcp.AdmissionOutcomeAbsent), response.JSON200.Epoch)
}

func (a *OrdinaryDeploymentAdmissionAttempt) receipt(disposition string, deploymentID uuid.UUID, outcome string) model.DeploymentAdmissionReceipt {
	return model.DeploymentAdmissionReceipt{
		OrgID: a.OrgID, AdmissionID: a.AdmissionID, ProjectUUID: a.ProjectUUID, EnvironmentUUID: a.EnvironmentUUID,
		ActorID: a.ActorID, RequestDigest: a.requestDigest, Mode: a.Mode, ModuleVersions: a.moduleVersions,
		CarryForwardModuleVersions: a.carryForwardModuleVersions, ConfirmedRestrictedVersionUUIDs: a.confirmedRestrictedVersionUUIDs,
		CoreEpoch: a.coreEpoch, Disposition: disposition, DeploymentID: uuid.NullUUID{UUID: deploymentID, Valid: deploymentID != uuid.Nil}, TerminalOutcome: outcome,
	}
}

func deploymentAdmissionRequest(receipt *model.DeploymentAdmissionReceipt) platformorchestratorcp.DeploymentAdmissionRequest {
	request := platformorchestratorcp.DeploymentAdmissionRequest{
		ActorId: receipt.ActorID, ProjectUuid: receipt.ProjectUUID, EnvironmentUuid: receipt.EnvironmentUUID,
		Mode: platformorchestratorcp.DeploymentAdmissionRequestMode(receipt.Mode), ModuleVersions: receipt.ModuleVersions,
		RequestDigest: receipt.RequestDigest,
	}
	if len(receipt.CarryForwardModuleVersions) > 0 {
		request.CarryForwardModuleVersions = &receipt.CarryForwardModuleVersions
	}
	if len(receipt.ConfirmedRestrictedVersionUUIDs) > 0 {
		request.ConfirmedRestrictedVersionUuids = &receipt.ConfirmedRestrictedVersionUUIDs
	}
	return request
}

func matchingAdmissionReceipt(receipt *platformorchestratorcp.DeploymentAdmissionReceipt, expected *model.DeploymentAdmissionReceipt, state platformorchestratorcp.DeploymentAdmissionReceiptState, outcome string) bool {
	return receipt.AdmissionId == expected.AdmissionID && receipt.ProjectUuid == expected.ProjectUUID &&
		receipt.EnvironmentUuid == expected.EnvironmentUUID && receipt.RequestDigest == expected.RequestDigest &&
		receipt.Mode == string(expected.Mode) && receipt.State == state && receipt.Epoch > 0 &&
		receipt.Outcome != nil && *receipt.Outcome == outcome
}

func (s *OrdinaryDeploymentAdmissionService) ReconcileUnsettled(ctx context.Context, limit int) error {
	if s == nil {
		return nil
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	receipts, err := model.ListUnsettledDeploymentAdmissionReceipts(ctx, tx, limit, time.Now().UTC().Add(-DeploymentAdmissionRecoveryGrace))
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	for index := range receipts {
		var err error
		if receipts[index].Disposition == "committed" && receipts[index].DeploymentID.Valid {
			err = s.SettleDeployment(ctx, receipts[index].DeploymentID.UUID)
		} else {
			err = s.sealAbsent(ctx, receipts[index])
		}
		if err != nil {
			if _, committed := model.IsErrConflict(err); committed {
				continue
			}
			return err
		}
	}
	return nil
}

func (s *OrdinaryDeploymentAdmissionService) SettleDeployment(ctx context.Context, deploymentID uuid.UUID) error {
	if s == nil {
		return nil
	}
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := model.GetDeploymentAdmissionReceiptByDeployment(ctx, tx, deploymentID)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return nil
		}
		return err
	}
	if receipt.CoreSettledAt.Valid {
		return nil
	}
	deployment, _, _, _, err := s.Database.GetDeployment(ctx, tx, receipt.OrgID, deploymentID, model.GetModeDefault)
	if err != nil {
		return err
	}
	var outcome platformorchestratorcp.SettleDeploymentAdmissionJSONBodyOutcome
	switch deployment.Status {
	case model.DeploymentStatusSucceeded:
		outcome = platformorchestratorcp.AdmissionOutcomeSucceeded
	case model.DeploymentStatusFailed:
		outcome = platformorchestratorcp.AdmissionOutcomeFailed
	default:
		return nil
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.settle(ctx, receipt, outcome)
}

func (s *OrdinaryDeploymentAdmissionService) settle(ctx context.Context, receipt *model.DeploymentAdmissionReceipt, outcome platformorchestratorcp.SettleDeploymentAdmissionJSONBodyOutcome) error {
	response, err := s.Core.SettleDeploymentAdmissionWithResponse(ctx, receipt.OrgID, receipt.AdmissionID,
		platformorchestratorcp.SettleDeploymentAdmissionJSONRequestBody{
			ProjectUuid: receipt.ProjectUUID, EnvironmentUuid: receipt.EnvironmentUUID,
			RequestDigest: receipt.RequestDigest, Outcome: outcome,
		}, s.bearer())
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDeploymentAdmissionUnavailable, err)
	}
	if response.StatusCode() != http.StatusOK || response.JSON200 == nil ||
		!matchingAdmissionReceipt(response.JSON200, receipt, platformorchestratorcp.AdmissionStateSettled, string(outcome)) ||
		response.JSON200.Epoch != receipt.CoreEpoch {
		return fmt.Errorf("%w: Core rejected or mismatched settlement", ErrDeploymentAdmissionUnavailable)
	}
	return s.markCoreSettled(ctx, receipt, string(outcome), response.JSON200.Epoch)
}

func (s *OrdinaryDeploymentAdmissionService) markCoreSettled(ctx context.Context, receipt *model.DeploymentAdmissionReceipt, outcome string, coreEpoch int64) error {
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := model.MarkDeploymentAdmissionCoreSettled(ctx, tx, receipt.OrgID, receipt.AdmissionID, outcome, coreEpoch); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *OrdinaryDeploymentAdmissionService) bearer() platformorchestratorcp.RequestEditorFn {
	return func(_ context.Context, request *http.Request) error {
		request.Header.Set("Authorization", "Bearer "+s.BearerToken)
		return nil
	}
}
