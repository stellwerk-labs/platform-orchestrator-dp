package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

const (
	ExternalDeploymentAdmissionPath           = "/orgs/:orgId/plugins/:pluginId/external-deployments"
	maxExternalDeploymentAdmissionBodyBytes   = 1 << 20
	externalDeploymentInvalidRequestCode      = "invalid_request"
	externalDeploymentCoreUnavailableCode     = "core_unavailable"
	externalDeploymentUnavailableCode         = "external_deployment_unavailable"
	externalDeploymentFailedCode              = "external_deployment_failed"
	externalInvalidPluginContextCode          = "invalid_plugin_context"
	externalInvalidPluginContextMessage       = "a valid request-bound plugin context is required"
	externalInvalidActorMessage               = "plugin context actor is invalid"
	externalDeploymentNotFoundCode            = "deployment_not_found"
	externalQueryUnsupportedMessage           = "query parameters are not supported"
	externalInvalidPathIdentityMessage        = "organization or plugin identity is invalid"
	externalEnvironmentIdentityChangedCode    = "environment_identity_changed"
	externalEnvironmentIdentityChangedMessage = "authorized Environment identity is no longer current"
)

var (
	externalPluginIDPattern        = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$`)
	externalPlanFingerprintPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
)

type ExternalDeploymentHTTPAdapter struct {
	Issuer             string
	VerificationKey    ed25519.PublicKey
	Admission          ExternalDeploymentAdmissionService
	Materials          ExternalOperationMaterialService
	ControlPlaneClient platformorchestratorcp.ClientWithResponsesInterface
	EventPublisher     hmessaging.Publisher
	RunnerLogsReader   RunnerLogsReader
	Logger             *zap.Logger
	Now                func() time.Time
}

type externalDeploymentErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func (a *ExternalDeploymentHTTPAdapter) Handle(c echo.Context) error {
	if err := a.validateAdmissionConfiguration(); err != nil {
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentUnavailableCode, Message: "external deployment admission is unavailable"})
	}
	request := c.Request()
	requestContext, cancel := context.WithTimeout(request.Context(), RequestHandlerTimeout)
	defer cancel()
	request = request.WithContext(requestContext)
	c.SetRequest(request)
	if request.URL.RawQuery != "" || request.URL.Fragment != "" {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: externalQueryUnsupportedMessage})
	}
	if contentType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type")); err != nil || contentType != "application/json" {
		return c.JSON(http.StatusUnsupportedMediaType, externalDeploymentErrorResponse{Error: "invalid_content_type", Message: "Content-Type must be application/json"})
	}
	idempotencyKey, ok := singleHeader(request.Header, "Idempotency-Key")
	if !ok || idempotencyKey == "" || len(idempotencyKey) > 128 {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: "invalid_idempotency_key", Message: "one non-empty Idempotency-Key of at most 128 characters is required"})
	}
	assertion, ok := singleHeader(request.Header, plugincontext.Header)
	if !ok || assertion == "" {
		return c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidPluginContextMessage})
	}

	request.Body = http.MaxBytesReader(c.Response(), request.Body, maxExternalDeploymentAdmissionBodyBytes)
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "request body is invalid or too large"})
	}
	orgID, pluginID := c.Param("orgId"), c.Param("pluginId")
	if !validExternalPathIdentity(orgID, pluginID) {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: externalInvalidPathIdentityMessage})
	}
	claims, err := plugincontext.Verify(a.VerificationKey, assertion, plugincontext.Request{
		Issuer:         a.Issuer,
		Audience:       ExternalDeploymentAudience,
		PluginID:       pluginID,
		OrgID:          orgID,
		Method:         http.MethodPost,
		Path:           request.URL.EscapedPath(),
		Body:           bodyBytes,
		OperationID:    ExternalDeploymentAction,
		IdempotencyKey: idempotencyKey,
	}, a.now())
	if err != nil {
		return c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidPluginContextMessage})
	}

	var body ExternalDeploymentAdmissionBody
	decoder := json.NewDecoder(bytes.NewReader(bodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || requireJSONEOF(decoder) != nil {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "request body must be one strict JSON object"})
	}
	if body.OperationRef == uuid.Nil || body.Fence <= 0 {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "operation_ref must be a UUID and fence must be a positive signed 64-bit generation"})
	}
	mode, serviceAction, err := validateExternalDeploymentBody(body)
	if err != nil {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: "invalid_deployment", Message: err.Error()})
	}

	environmentResponse, err := a.ControlPlaneClient.GetEnvironmentWithResponse(
		request.Context(), orgID, body.ProjectId, body.EnvId,
	)
	if err != nil {
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentCoreUnavailableCode, Message: "Core environment lookup is unavailable"})
	}
	if environmentResponse.StatusCode() == http.StatusNotFound {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "environment_not_found", Message: "target environment does not exist"})
	}
	if environmentResponse.StatusCode() != http.StatusOK || environmentResponse.JSON200 == nil || environmentResponse.JSON200.ProjectUuid == nil {
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentCoreUnavailableCode, Message: "Core environment lookup did not return an authoritative target"})
	}
	environment := *environmentResponse.JSON200
	if environment.ProjectId != body.ProjectId || environment.Id != body.EnvId ||
		*environment.ProjectUuid != body.ProjectUuid || environment.Uuid != body.EnvironmentUuid {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: externalEnvironmentIdentityChangedCode, Message: externalEnvironmentIdentityChangedMessage})
	}

	admissionRequest := ExternalDeploymentAdmissionRequest{
		Claims:        claims,
		OperationRef:  body.OperationRef.String(),
		Fence:         body.Fence,
		ServiceAction: serviceAction,
		ProjectUUID:   body.ProjectUuid,
		EnvUUID:       body.EnvironmentUuid,
		ProjectID:     body.ProjectId,
		EnvID:         body.EnvId,
		Create: func(ctx context.Context, tx model.Tx, actorID uuid.UUID) (*ExternalDeploymentCreation, error) {
			material, err := a.Materials.GetOrCreate(ctx, tx, orgID, pluginID, body.OperationRef)
			if err != nil {
				return nil, errors.Join(ErrExternalDeploymentUnavailable, err)
			}
			if mode == model.DeploymentModeDeploy {
				return a.createExternalDeploymentFromSnapshot(ctx, tx, actorID, orgID, pluginID, body, environment, material.Recipient)
			}
			return a.createExternalRollback(ctx, tx, actorID, orgID, pluginID, body, environment, material.Recipient)
		},
	}
	result, err := a.Admission.Admit(request.Context(), admissionRequest)
	if err != nil {
		return a.writeAdmissionError(c, err)
	}
	if !result.Replayed {
		reliableoutbox.OptimisticPublish(request.Context(), a.logger(), a.Admission.Database.AsReliableOutboxStore(), a.EventPublisher, result.PendingEvents)
	}
	status := http.StatusCreated
	if result.Replayed {
		status = http.StatusOK
	}
	return c.JSON(status, ExternalDeploymentAdmissionResponse{DeploymentId: result.Deployment.Id, Replayed: result.Replayed})
}

func (a *ExternalDeploymentHTTPAdapter) validateConfiguration() error {
	if a == nil || a.Issuer == "" || len(a.VerificationKey) != ed25519.PublicKeySize ||
		a.Admission.Database == nil || a.Admission.Authorizer == nil || a.ControlPlaneClient == nil || a.EventPublisher == nil {
		return errors.New("external deployment HTTP adapter is not configured")
	}
	return nil
}

func (a *ExternalDeploymentHTTPAdapter) validateAdmissionConfiguration() error {
	if err := a.validateConfiguration(); err != nil {
		return err
	}
	if a.Materials.Vault == nil {
		return errors.New("external deployment material service is not configured")
	}
	return nil
}

func (a *ExternalDeploymentHTTPAdapter) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func (a *ExternalDeploymentHTTPAdapter) logger() *zap.Logger {
	if a.Logger != nil {
		return a.Logger
	}
	return zap.NewNop()
}

func (a *ExternalDeploymentHTTPAdapter) writeAdmissionError(c echo.Context, err error) error {
	if errors.Is(err, ErrExternalDeploymentUnauthorized) {
		return c.JSON(http.StatusForbidden, externalDeploymentErrorResponse{Error: "external_operation_not_authorized", Message: "Core did not authorize this deployment dispatch"})
	}
	if errors.Is(err, ErrExternalDeploymentUnavailable) {
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentUnavailableCode, Message: "an authoritative external deployment dependency is unavailable"})
	}
	if badRequest, ok := model.IsErrBadRequest(err); ok {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: "invalid_deployment", Message: badRequest.Message})
	}
	if conflict, ok := model.IsErrConflict(err); ok {
		code := conflict.Code
		if code == "" {
			code = "deployment_conflict"
		}
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: code, Message: conflict.Message})
	}
	a.logger().Error("external deployment admission failed", zap.Error(err))
	return c.JSON(http.StatusInternalServerError, externalDeploymentErrorResponse{Error: externalDeploymentFailedCode, Message: "external deployment admission failed"})
}

func validateExternalDeploymentBody(body ExternalDeploymentAdmissionBody) (model.DeploymentMode, string, error) {
	if body.FirstForwardDeploymentId != nil && *body.FirstForwardDeploymentId == uuid.Nil {
		return "", "", errors.New("first_forward_deployment_id must be a nonzero UUID")
	}
	if body.ProjectId == "" || body.EnvId == "" || body.ProjectUuid == uuid.Nil || body.EnvironmentUuid == uuid.Nil {
		return "", "", errors.New("project and Environment identities are required")
	}
	if body.RunnerLogLevel != nil && !body.RunnerLogLevel.Valid() {
		return "", "", errors.New("runner_log_level is invalid")
	}
	switch body.Mode {
	case ExternalDeploymentAdmissionBodyModeDeploy:
		if body.Plan == nil || body.Rollback != nil || body.Plan.SnapshotId == uuid.Nil ||
			!externalPlanFingerprintPattern.MatchString(body.Plan.Fingerprint) ||
			len(body.Plan.ExpectedModuleVersions) == 0 || len(body.Plan.ExpectedModuleVersions) > 1000 {
			return "", "", errors.New("deploy requires one valid immutable plan and no rollback reference")
		}
		for moduleRef, version := range body.Plan.ExpectedModuleVersions {
			moduleID, err := uuid.Parse(moduleRef)
			if err != nil || moduleID == uuid.Nil || version == uuid.Nil {
				return "", "", errors.New("expected_module_versions must map Module UUIDs to Version UUIDs")
			}
		}
		return model.DeploymentModeDeploy, ExternalDeploymentAction, nil
	case ExternalDeploymentAdmissionBodyModeRollback:
		if body.Rollback == nil || body.Plan != nil || body.Rollback.ForwardDeploymentId == uuid.Nil {
			return "", "", errors.New("rollback requires one forward deployment reference and no plan")
		}
		if body.Rollback.RetryOfRollbackDeploymentId != nil && *body.Rollback.RetryOfRollbackDeploymentId == uuid.Nil {
			return "", "", errors.New("retry_of_rollback_deployment_id must be a nonzero UUID")
		}
		return model.DeploymentModeRollback, ExternalDeploymentRollbackAction, nil
	default:
		return "", "", errors.New("mode must be deploy or rollback")
	}
}

func (a *ExternalDeploymentHTTPAdapter) createExternalDeploymentFromSnapshot(
	ctx context.Context, tx model.Tx, actorID uuid.UUID, orgID, pluginID string, body ExternalDeploymentAdmissionBody,
	environment platformorchestratorcp.Environment, recipient string,
) (*ExternalDeploymentCreation, error) {
	scope := externalRollbackScope(orgID, pluginID, body)
	anchor, err := a.operationRollbackAnchor(ctx, tx, scope, true)
	if err != nil {
		return nil, err
	}
	if body.FirstForwardDeploymentId != nil && (anchor == nil || *body.FirstForwardDeploymentId != anchor.FirstForwardDeploymentID) {
		return nil, model.NewErrConflict("first forward assertion differs from immutable operation lineage")
	}
	snapshot, err := model.GetExternalDeploymentPlanSnapshotByID(ctx, tx, orgID, pluginID, body.Plan.SnapshotId)
	if err != nil {
		return nil, err
	}
	if snapshot.ActorID != actorID || snapshot.ProjectID != body.ProjectId || snapshot.ProjectUUID != body.ProjectUuid ||
		snapshot.EnvID != body.EnvId || snapshot.EnvironmentUUID != body.EnvironmentUuid || snapshot.Fingerprint != body.Plan.Fingerprint ||
		deploymentPlanFingerprint(DeploymentPlanCapture{
			BaseDeploymentID: snapshot.BaseDeploymentID, Manifest: snapshot.Manifest, Graph: snapshot.Graph,
			Diff: snapshot.Diff, ModuleArtifacts: snapshot.ModuleArtifacts,
		}) != snapshot.Fingerprint {
		return nil, model.NewErrConflict("external deployment plan snapshot identity or content changed")
	}
	explicitVersions := make(map[string]string, len(snapshot.ExplicitTargets))
	expectedVersions := make(map[string]uuid.UUID, len(snapshot.ExplicitTargets))
	targetsByModuleID := make(map[string]model.ExternalDeploymentPlanTarget, len(snapshot.ExplicitTargets))
	for _, target := range snapshot.ExplicitTargets {
		if target.ModuleID == "" || target.ModuleUUID == uuid.Nil || target.Version == "" || target.VersionUUID == uuid.Nil {
			return nil, model.NewErrConflict("external deployment plan has incomplete target identity")
		}
		moduleRef := target.ModuleUUID.String()
		if _, duplicate := expectedVersions[moduleRef]; duplicate {
			return nil, model.NewErrConflict("external deployment plan repeats a Module identity")
		}
		explicitVersions[target.ModuleID] = target.Version
		expectedVersions[moduleRef] = target.VersionUUID
		targetsByModuleID[target.ModuleID] = target
	}
	if !reflect.DeepEqual(expectedVersions, body.Plan.ExpectedModuleVersions) {
		return nil, model.NewErrConflict("external deployment target versions differ from the immutable plan")
	}
	var manifest DeploymentManifest
	if err := json.Unmarshal(snapshot.Manifest, &manifest); err != nil {
		return nil, errors.New("external deployment plan manifest is invalid")
	}
	deployment, pending, _, err := CreateDeploymentWithOptions(
		ctx, a.logger(), actorID, orgID, body.ProjectId, body.EnvId, environment,
		model.DeploymentModeDeploy, opt.Empty[uuid.UUID](), manifest, &recipient, &recipient, nil, false,
		externalRunnerLogLevel(body.RunnerLogLevel), a.ControlPlaneClient, a.Admission.Database, tx,
		DeploymentOptions{
			ExplicitModuleVersions: explicitVersions,
			ModuleOperationID:      opt.Of(body.OperationRef),
			AuthorizeModuleVersion: func(moduleID string, explicitlySelected bool, artifact catalogueArtifact) error {
				if !explicitlySelected {
					return nil
				}
				target, expected := targetsByModuleID[moduleID]
				if !expected {
					return model.NewErrConflict("resolved Module target was not present in the immutable plan")
				}
				if artifact.ModuleUUID != target.ModuleUUID || artifact.VersionUUID != target.VersionUUID ||
					artifact.SemanticVersion != target.SemanticVersion || artifact.Digest != target.ArtifactDigest {
					return model.NewErrConflict("resolved Module target differs from the immutable plan")
				}
				return nil
			},
			ValidatePlan: func(captured DeploymentPlanCapture) error {
				if captured.BaseDeploymentID != snapshot.BaseDeploymentID || deploymentPlanFingerprint(captured) != snapshot.Fingerprint ||
					!reflect.DeepEqual(captured.ModuleArtifacts, snapshot.ModuleArtifacts) {
					return model.NewErrConflict("current deployment plan differs from the immutable snapshot")
				}
				return nil
			},
		},
	)
	if err != nil {
		return nil, err
	}
	if anchor == nil {
		scope.FirstForwardDeploymentID, scope.RollbackBaseDeploymentID = deployment.Id, snapshot.BaseDeploymentID
		if err := a.validateRollbackBase(ctx, tx, scope); err != nil {
			return nil, err
		}
		if err := model.CreateExternalOperationRollbackAnchor(ctx, tx, scope); err != nil {
			return nil, err
		}
	}
	return &ExternalDeploymentCreation{
		Deployment: deployment, PendingEvents: pending, PlanSnapshotID: opt.Of(snapshot.ID), BaseDeploymentID: opt.Of(snapshot.BaseDeploymentID),
	}, nil
}

func (a *ExternalDeploymentHTTPAdapter) createExternalRollback(
	ctx context.Context, tx model.Tx, actorID uuid.UUID, orgID, pluginID string, body ExternalDeploymentAdmissionBody,
	environment platformorchestratorcp.Environment, recipient string,
) (*ExternalDeploymentCreation, error) {
	scope := externalRollbackScope(orgID, pluginID, body)
	anchor, err := a.operationRollbackAnchor(ctx, tx, scope, true)
	if err != nil {
		return nil, err
	}
	if anchor == nil || body.FirstForwardDeploymentId != nil && *body.FirstForwardDeploymentId != anchor.FirstForwardDeploymentID {
		return nil, model.NewErrConflict("rollback requires the immutable first forward lineage")
	}
	forwardReceipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, orgID, pluginID, body.Rollback.ForwardDeploymentId)
	if err != nil {
		return nil, model.NewErrConflict("forward external deployment receipt was not found")
	}
	if forwardReceipt.OperationRef != body.OperationRef.String() || forwardReceipt.ServiceAction != ExternalDeploymentAction ||
		forwardReceipt.ProjectID != body.ProjectId || forwardReceipt.EnvID != body.EnvId ||
		!forwardReceipt.ProjectUUID.IsSet() || forwardReceipt.ProjectUUID.Must() != body.ProjectUuid ||
		!forwardReceipt.EnvironmentUUID.IsSet() || forwardReceipt.EnvironmentUUID.Must() != body.EnvironmentUuid ||
		!forwardReceipt.BaseDeploymentID.IsSet() {
		return nil, model.NewErrConflict("forward deployment is outside this external operation or lacks a captured base")
	}
	forward, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, orgID, body.Rollback.ForwardDeploymentId, model.GetModeDefault)
	if err != nil || forward.Mode != model.DeploymentModeDeploy || !forward.CompletedAt.IsSet() ||
		(forward.Status != model.DeploymentStatusSucceeded && forward.Status != model.DeploymentStatusFailed) ||
		forward.DeploymentEnvUuid != body.EnvironmentUuid || forward.ProjectId != body.ProjectId || forward.EnvId != body.EnvId {
		return nil, model.NewErrConflict("forward deployment is not a stopped terminal Deployment in this Environment")
	}
	baseID := anchor.RollbackBaseDeploymentID
	expectedHead := forward.Id
	retryOf := opt.Empty[uuid.UUID]()
	if body.Rollback.RetryOfRollbackDeploymentId != nil {
		previousID := *body.Rollback.RetryOfRollbackDeploymentId
		previousReceipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(ctx, tx, orgID, pluginID, previousID)
		if err != nil || !externalReceiptOwnsScope(previousReceipt, scope) || previousReceipt.ServiceAction != ExternalDeploymentRollbackAction ||
			!previousReceipt.BaseDeploymentID.IsSet() || previousReceipt.BaseDeploymentID.Must() != baseID ||
			!previousReceipt.ForwardDeploymentID.IsSet() || previousReceipt.ForwardDeploymentID.Must() != forward.Id {
			return nil, model.NewErrConflict("rollback retry predecessor is outside this operation or original base")
		}
		previous, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, orgID, previousID, model.GetModeDefault)
		if err != nil || previous.Mode != model.DeploymentModeRollback || previous.Status != model.DeploymentStatusFailed || !previous.CompletedAt.IsSet() ||
			previous.DeploymentEnvUuid != body.EnvironmentUuid || previous.ProjectId != body.ProjectId || previous.EnvId != body.EnvId ||
			!previous.RollbackToId.IsSet() || previous.RollbackToId.Must() != baseID {
			return nil, model.NewErrConflict("rollback retry requires this operation's terminal failed rollback to the original base")
		}
		expectedHead, retryOf = previousID, opt.Of(previousID)
	}
	base, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, orgID, baseID, model.GetModeDefault)
	if err != nil || base.Status != model.DeploymentStatusSucceeded || !base.CompletedAt.IsSet() || base.DeploymentEnvUuid != body.EnvironmentUuid || base.ProjectId != body.ProjectId || base.EnvId != body.EnvId {
		return nil, model.NewErrConflict("captured rollback base is not retained as a succeeded Deployment")
	}
	expectedVersions, err := immutableModuleVersions(base.ModuleArtifacts)
	if err != nil {
		return nil, err
	}
	confirmations := make([]uuid.UUID, 0, len(base.ModuleArtifacts))
	for _, artifact := range base.ModuleArtifacts {
		confirmations = append(confirmations, artifact.VersionUUID)
	}
	deployment, pending, _, err := CreateDeploymentWithOptions(
		ctx, a.logger(), actorID, orgID, body.ProjectId, body.EnvId, environment,
		model.DeploymentModeRollback, opt.Of(baseID), DeploymentManifest{}, &recipient, &recipient, nil, false,
		externalRunnerLogLevel(body.RunnerLogLevel), a.ControlPlaneClient, a.Admission.Database, tx,
		DeploymentOptions{
			RestrictedConfirmations:     confirmations,
			ExpectedCurrentDeploymentID: opt.Of(expectedHead),
			RetryOfRollbackDeploymentID: retryOf,
			ValidatePlan: func(captured DeploymentPlanCapture) error {
				versions, versionErr := immutableModuleVersions(captured.ModuleArtifacts)
				if versionErr != nil || captured.BaseDeploymentID != baseID || !reflect.DeepEqual(versions, expectedVersions) {
					return model.NewErrConflict("rollback resolution differs from the captured effective base")
				}
				return nil
			},
		},
	)
	if err != nil {
		return nil, err
	}
	return &ExternalDeploymentCreation{Deployment: deployment, PendingEvents: pending, BaseDeploymentID: opt.Of(baseID),
		ForwardDeploymentID: opt.Of(forward.Id), RetryOfRollbackDeploymentID: retryOf}, nil
}

func externalRunnerLogLevel(level *ExternalDeploymentAdmissionBodyRunnerLogLevel) *DeploymentCreateBodyRunnerLogLevel {
	if level == nil {
		return nil
	}
	converted := DeploymentCreateBodyRunnerLogLevel(*level)
	return &converted
}

func immutableModuleVersions(artifacts []model.ModuleArtifactRequirement) (map[string]uuid.UUID, error) {
	versions := make(map[string]uuid.UUID, len(artifacts))
	for _, artifact := range artifacts {
		if artifact.ModuleUUID == uuid.Nil || artifact.VersionUUID == uuid.Nil {
			return nil, model.NewErrConflict("Deployment lacks immutable Module Version identity")
		}
		key := artifact.ModuleUUID.String()
		if prior, duplicate := versions[key]; duplicate && prior != artifact.VersionUUID {
			return nil, model.NewErrConflict("Deployment contains conflicting Module Version identities")
		}
		versions[key] = artifact.VersionUUID
	}
	return versions, nil
}

func validExternalPathIdentity(orgID, pluginID string) bool {
	return orgID != "" && len(orgID) <= 255 && !strings.ContainsAny(orgID, "/\\") && orgID != "." && orgID != ".." &&
		len(pluginID) <= 255 && externalPluginIDPattern.MatchString(pluginID)
}

func singleHeader(header http.Header, name string) (string, bool) {
	values := header.Values(name)
	returnValue := ""
	if len(values) == 1 {
		returnValue = strings.TrimSpace(values[0])
	}
	return returnValue, len(values) == 1
}
