package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

const (
	ExternalDeploymentAdmissionPath         = "/orgs/:orgId/plugins/:pluginId/external-deployments"
	maxExternalDeploymentAdmissionBodyBytes = 8 * 1024 * 1024
	externalDeploymentInvalidRequestCode    = "invalid_request"
	externalDeploymentCoreUnavailableCode   = "core_unavailable"
	externalDeploymentUnavailableCode       = "external_deployment_unavailable"
	externalDeploymentFailedCode            = "external_deployment_failed"
	externalInvalidPluginContextCode        = "invalid_plugin_context"
	externalInvalidPluginContextMessage     = "a valid request-bound plugin context is required"
	externalInvalidActorMessage             = "plugin context actor is invalid"
	externalDeploymentNotFoundCode          = "deployment_not_found"
	externalQueryUnsupportedMessage         = "query parameters are not supported"
	externalInvalidPathIdentityMessage      = "organization or plugin identity is invalid"
	externalEnvironmentIdentityChangedCode  = "environment_identity_changed"
)

var externalPluginIDPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$`)

type ExternalDeploymentHTTPAdapter struct {
	Issuer             string
	VerificationKey    ed25519.PublicKey
	Admission          ExternalDeploymentAdmissionService
	ControlPlaneClient platformorchestratorcp.ClientWithResponsesInterface
	EventPublisher     hmessaging.Publisher
	Logger             *zap.Logger
	Now                func() time.Time
}

// ExternalDeploymentAdmissionBody binds a normal Deployment request to one
// Core-owned business operation and fencing generation.
type ExternalDeploymentAdmissionBody struct {
	OperationRef string               `json:"operation_ref"`
	Fence        uint64               `json:"fence"`
	Deployment   DeploymentCreateBody `json:"deployment"`
}

// ExternalDeploymentAdmissionResponse returns the ordinary Deployment identity
// and whether the durable external-operation receipt was replayed.
type ExternalDeploymentAdmissionResponse struct {
	DeploymentID uuid.UUID `json:"deployment_id"`
	Replayed     bool      `json:"replayed"`
}

type externalDeploymentErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

func (a *ExternalDeploymentHTTPAdapter) Handle(c echo.Context) error {
	if err := a.validateConfiguration(); err != nil {
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
	operationID, err := uuid.Parse(body.OperationRef)
	if err != nil || operationID == uuid.Nil || body.Fence == 0 || body.Fence > math.MaxInt64 {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "operation_ref must be a UUID and fence must be a positive signed 64-bit generation"})
	}
	mode, serviceAction, manifest, err := prepareExternalDeployment(body.Deployment)
	if err != nil {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: "invalid_deployment", Message: err.Error()})
	}

	environmentResponse, err := a.ControlPlaneClient.GetEnvironmentWithResponse(
		request.Context(), orgID, body.Deployment.ProjectId, body.Deployment.EnvId,
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
	if environment.ProjectId != body.Deployment.ProjectId || environment.Id != body.Deployment.EnvId {
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentCoreUnavailableCode, Message: "Core environment lookup returned mismatched ownership"})
	}

	admissionRequest := ExternalDeploymentAdmissionRequest{
		Claims:        claims,
		OperationRef:  operationID.String(),
		Fence:         int64(body.Fence),
		ServiceAction: serviceAction,
		ProjectUUID:   *environment.ProjectUuid,
		EnvUUID:       environment.Uuid,
		ProjectID:     body.Deployment.ProjectId,
		EnvID:         body.Deployment.EnvId,
		Create: func(ctx context.Context, tx model.Tx, actorID uuid.UUID) (*model.DeploymentSummary, []*hstandardoutbox.PendingEventMessage, error) {
			deployment, pending, _, err := CreateDeploymentWithOptions(
				ctx, a.logger(), actorID, orgID, body.Deployment.ProjectId, body.Deployment.EnvId,
				environment, mode, opt.OfRef(body.Deployment.RollbackToDeploymentId), manifest,
				body.Deployment.EncryptedOutputsRecipient, body.Deployment.EncryptedLogsRecipient, nil,
				false, body.Deployment.RunnerLogLevel, a.ControlPlaneClient, a.Admission.Database, tx,
				DeploymentOptions{
					ExplicitModuleVersions:  body.Deployment.ModuleVersions,
					RestrictedConfirmations: body.Deployment.ConfirmRestrictedModuleVersionUuids,
					AuthorizeModuleVersion:  func(string, bool, catalogueArtifact) error { return nil },
				},
			)
			return deployment, pending, err
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
	return c.JSON(status, ExternalDeploymentAdmissionResponse{DeploymentID: result.Deployment.Id, Replayed: result.Replayed})
}

func (a *ExternalDeploymentHTTPAdapter) validateConfiguration() error {
	if a == nil || a.Issuer == "" || len(a.VerificationKey) != ed25519.PublicKeySize ||
		a.Admission.Database == nil || a.Admission.Authorizer == nil || a.ControlPlaneClient == nil || a.EventPublisher == nil {
		return errors.New("external deployment HTTP adapter is not configured")
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
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentCoreUnavailableCode, Message: "Core operation authorization is unavailable"})
	}
	if badRequest, ok := model.IsErrBadRequest(err); ok {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: "invalid_deployment", Message: badRequest.Message})
	}
	if conflict, ok := model.IsErrConflict(err); ok {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "deployment_conflict", Message: conflict.Message})
	}
	a.logger().Error("external deployment admission failed", zap.Error(err))
	return c.JSON(http.StatusInternalServerError, externalDeploymentErrorResponse{Error: externalDeploymentFailedCode, Message: "external deployment admission failed"})
}

func prepareExternalDeployment(body DeploymentCreateBody) (model.DeploymentMode, string, DeploymentManifest, error) {
	if body.IsDryRun || (body.PlanOnly != nil && *body.PlanOnly) || body.Mode == DeploymentCreateBodyModePlanOnly {
		return "", "", DeploymentManifest{}, errors.New("dry-run and plan-only requests use the separate planning contract")
	}
	if body.ProjectId == "" || body.EnvId == "" {
		return "", "", DeploymentManifest{}, errors.New("project_id and env_id are required")
	}
	if body.EncryptedOutputsRecipient != nil {
		if _, err := age.ParseX25519Recipient(*body.EncryptedOutputsRecipient); err != nil {
			return "", "", DeploymentManifest{}, errors.New("encrypted_outputs_recipient is not a valid age X25519 recipient")
		}
	}
	if body.EncryptedLogsRecipient != nil {
		if _, err := age.ParseX25519Recipient(*body.EncryptedLogsRecipient); err != nil {
			return "", "", DeploymentManifest{}, errors.New("encrypted_logs_recipient is not a valid age X25519 recipient")
		}
	}

	switch body.Mode {
	case DeploymentCreateBodyModeDeploy:
		if body.Manifest == nil {
			return "", "", DeploymentManifest{}, errors.New("manifest is required for a deploy request")
		}
		if body.RollbackToDeploymentId != nil {
			return "", "", DeploymentManifest{}, errors.New("rollback_to_deployment_id is not valid for a deploy request")
		}
		manifest := *body.Manifest
		if err := normalizeExternalDeploymentManifest(&manifest); err != nil {
			return "", "", DeploymentManifest{}, err
		}
		return model.DeploymentModeDeploy, ExternalDeploymentAction, manifest, nil
	case DeploymentCreateBodyModeRollback:
		if body.Manifest != nil {
			return "", "", DeploymentManifest{}, errors.New("manifest is not valid for a rollback request")
		}
		if body.RollbackToDeploymentId == nil || *body.RollbackToDeploymentId == uuid.Nil {
			return "", "", DeploymentManifest{}, errors.New("rollback_to_deployment_id is required for a rollback request")
		}
		if len(body.ModuleVersions) != 0 {
			return "", "", DeploymentManifest{}, errors.New("module_versions are not valid for a rollback request")
		}
		return model.DeploymentModeRollback, ExternalDeploymentRollbackAction, DeploymentManifest{}, nil
	default:
		return "", "", DeploymentManifest{}, errors.New("mode must be deploy or rollback")
	}
}

func normalizeExternalDeploymentManifest(manifest *DeploymentManifest) error {
	for name, workload := range manifest.Workloads {
		if !validWorkloadNamePattern.MatchString(name) {
			return fmt.Errorf("workload name %q is invalid", name)
		}
		if workload.Variables != nil {
			if workload.Outputs != nil {
				return errors.New("variables and outputs cannot be used together, use outputs")
			}
			workload.Outputs = workload.Variables
			workload.Variables = nil
			manifest.Workloads[name] = workload
		}
	}
	for alias := range manifest.Shared {
		if !validSharedAliasPattern.MatchString(alias) {
			return fmt.Errorf("shared resource alias %q is invalid", alias)
		}
	}
	return nil
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
