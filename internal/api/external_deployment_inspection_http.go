package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

const (
	ExternalDeploymentReadinessPath    = "/orgs/:orgId/plugins/:pluginId/external-deployment-readiness"
	ExternalDeploymentPlanningPath     = "/orgs/:orgId/plugins/:pluginId/external-deployment-plans"
	ExternalDeploymentReadPath         = "/orgs/:orgId/plugins/:pluginId/projects/:projectUUID/envs/:environmentUUID/external-deployments/:deploymentId"
	ExternalDeploymentDispatchReadPath = "/orgs/:orgId/plugins/:pluginId/projects/:projectUUID/envs/:environmentUUID/external-deployment-dispatches/:dispatchId"
	ExternalDeploymentReadinessAction  = "external-deployment.readiness"
	ExternalDeploymentPlanningAction   = "external-deployment.plan"
	ExternalDeploymentReadAction       = "external-deployment.read"
	maxExternalInspectionBodyBytes     = 1 << 20
	maxExternalReadinessEnvironments   = 1000
)

type externalEnvironmentRef struct {
	ProjectID       string    `json:"project_id"`
	EnvID           string    `json:"env_id"`
	ProjectUUID     uuid.UUID `json:"project_uuid"`
	EnvironmentUUID uuid.UUID `json:"environment_uuid"`
}

type externalDeploymentReadinessBody struct {
	Environments []externalEnvironmentRef `json:"environments"`
}

type externalModuleInventory struct {
	ModuleID        string     `json:"module_id"`
	ModuleUUID      *uuid.UUID `json:"module_uuid,omitempty"`
	Version         string     `json:"version"`
	VersionUUID     *uuid.UUID `json:"version_uuid,omitempty"`
	SemanticVersion string     `json:"semantic_version,omitempty"`
	ArtifactDigest  string     `json:"artifact_digest,omitempty"`
}

type externalEnvironmentReadiness struct {
	ProjectID                 string                    `json:"project_id"`
	EnvID                     string                    `json:"env_id"`
	ProjectUUID               uuid.UUID                 `json:"project_uuid"`
	EnvironmentUUID           *uuid.UUID                `json:"environment_uuid,omitempty"`
	State                     string                    `json:"state"`
	LatestDeploymentID        *uuid.UUID                `json:"latest_deployment_id,omitempty"`
	LatestStatus              string                    `json:"latest_status,omitempty"`
	LatestMode                string                    `json:"latest_mode,omitempty"`
	ActiveLeaseUntil          *time.Time                `json:"active_lease_until,omitempty"`
	EffectiveBaseDeploymentID *uuid.UUID                `json:"effective_base_deployment_id,omitempty"`
	ActiveModules             []externalModuleInventory `json:"active_modules"`
}

type externalDeploymentReadinessResponse struct {
	Items []externalEnvironmentReadiness `json:"items"`
}

type externalModuleSelection struct {
	ModuleUUID      uuid.UUID `json:"module_uuid"`
	Version         string    `json:"version"`
	VersionUUID     uuid.UUID `json:"version_uuid"`
	SemanticVersion string    `json:"semantic_version"`
	ArtifactDigest  string    `json:"artifact_digest"`
}

type externalDeploymentPlanBody struct {
	ProjectID       string                             `json:"project_id"`
	EnvID           string                             `json:"env_id"`
	ProjectUUID     uuid.UUID                          `json:"project_uuid"`
	EnvironmentUUID uuid.UUID                          `json:"environment_uuid"`
	ModuleVersions  map[string]externalModuleSelection `json:"module_versions"`
}

type externalManifestResourceStructure struct {
	Type      string   `json:"type"`
	Class     *string  `json:"class,omitempty"`
	ID        *string  `json:"id,omitempty"`
	ParamKeys []string `json:"param_keys"`
}

type externalManifestWorkloadStructure struct {
	OutputKeys []string                                     `json:"output_keys"`
	Resources  map[string]externalManifestResourceStructure `json:"resources"`
}

type externalManifestStructure struct {
	Shared    map[string]externalManifestResourceStructure `json:"shared"`
	Workloads map[string]externalManifestWorkloadStructure `json:"workloads"`
}

type externalGraphNodeStructure struct {
	ModuleID  string   `json:"module_id,omitempty"`
	Version   string   `json:"version,omitempty"`
	Deleted   bool     `json:"deleted,omitempty"`
	ParamKeys []string `json:"param_keys"`
	Drift     string   `json:"drift,omitempty"`
}

type externalGraphStructure struct {
	Nodes           map[string]externalGraphNodeStructure `json:"nodes"`
	Edges           map[string]map[string]string          `json:"edges"`
	Workloads       map[string]string                     `json:"workloads"`
	SharedResources map[string]string                     `json:"shared_resources"`
	LeafResources   []string                              `json:"leaf_resources"`
}

type externalDeploymentPlanResponse struct {
	SnapshotID                uuid.UUID                 `json:"snapshot_id"`
	EnvironmentUUID           uuid.UUID                 `json:"environment_uuid"`
	EffectiveBaseDeploymentID uuid.UUID                 `json:"effective_base_deployment_id"`
	Fingerprint               string                    `json:"fingerprint"`
	Manifest                  externalManifestStructure `json:"manifest"`
	Graph                     externalGraphStructure    `json:"graph"`
	Diff                      DeploymentDiff            `json:"diff"`
	ModuleVersions            []externalModuleInventory `json:"module_versions"`
}

type externalDeploymentStatusResponse struct {
	DeploymentID           uuid.UUID            `json:"deployment_id"`
	DispatchID             string               `json:"dispatch_id"`
	OperationRef           string               `json:"operation_ref"`
	Fence                  int64                `json:"fence"`
	RequestDigest          string               `json:"request_digest"`
	ProjectID              string               `json:"project_id"`
	EnvID                  string               `json:"env_id"`
	ProjectUUID            uuid.UUID            `json:"project_uuid"`
	EnvironmentUUID        uuid.UUID            `json:"environment_uuid"`
	Mode                   string               `json:"mode"`
	Status                 string               `json:"status"`
	StatusMessage          string               `json:"status_message"`
	ModuleVersions         map[string]uuid.UUID `json:"module_versions"`
	RollbackModuleVersions map[string]uuid.UUID `json:"rollback_module_versions"`
	CreatedAt              time.Time            `json:"created_at"`
	CompletedAt            *time.Time           `json:"completed_at,omitempty"`
	ReceiptCreatedAt       time.Time            `json:"receipt_created_at"`
}

func (a *ExternalDeploymentHTTPAdapter) HandleRead(c echo.Context) error {
	if err := a.validateConfiguration(); err != nil {
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentUnavailableCode, Message: "external deployment read is unavailable"})
	}
	request := c.Request()
	requestContext, cancel := context.WithTimeout(request.Context(), RequestHandlerTimeout)
	defer cancel()
	request = request.WithContext(requestContext)
	c.SetRequest(request)
	if request.URL.RawQuery != "" || request.URL.Fragment != "" {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: externalQueryUnsupportedMessage})
	}
	assertion, ok := singleHeader(request.Header, plugincontext.Header)
	if !ok || assertion == "" {
		return c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidPluginContextMessage})
	}
	orgID, pluginID := c.Param("orgId"), c.Param("pluginId")
	if !validExternalPathIdentity(orgID, pluginID) {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: externalInvalidPathIdentityMessage})
	}
	claims, err := plugincontext.Verify(a.VerificationKey, assertion, plugincontext.Request{
		Issuer: a.Issuer, Audience: ExternalDeploymentAudience, PluginID: pluginID, OrgID: orgID,
		Method: http.MethodGet, Path: request.URL.EscapedPath(), Body: nil, OperationID: ExternalDeploymentReadAction,
	}, a.now())
	if err != nil {
		return c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidPluginContextMessage})
	}
	if _, err := uuid.Parse(claims.ActorID); err != nil {
		return c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidActorMessage})
	}
	deploymentID, err := uuid.Parse(c.Param("deploymentId"))
	projectUUID, projectErr := uuid.Parse(c.Param("projectUUID"))
	environmentUUID, environmentErr := uuid.Parse(c.Param("environmentUUID"))
	if err != nil || projectErr != nil || environmentErr != nil || deploymentID == uuid.Nil || projectUUID == uuid.Nil || environmentUUID == uuid.Nil {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "deployment and environment identity is invalid"})
	}
	tx, err := a.Admission.Database.BeginTx(request.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	defer func() { _ = tx.Rollback() }()
	deployment, _, _, _, err := a.Admission.Database.GetDeployment(request.Context(), tx, orgID, deploymentID, model.GetModeDefault)
	if err != nil {
		if _, notFound := model.IsErrNotFound(err); notFound {
			return c.JSON(http.StatusNotFound, externalDeploymentErrorResponse{Error: externalDeploymentNotFoundCode, Message: "deployment was not found in the authorized environment"})
		}
		return a.writeInspectionError(c, err)
	}
	if deployment.DeploymentEnvUuid != environmentUUID {
		return c.JSON(http.StatusNotFound, externalDeploymentErrorResponse{Error: externalDeploymentNotFoundCode, Message: "deployment was not found in the authorized environment"})
	}
	receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(request.Context(), tx, orgID, pluginID, deploymentID)
	if err != nil {
		if _, notFound := model.IsErrNotFound(err); notFound {
			return c.JSON(http.StatusNotFound, externalDeploymentErrorResponse{Error: externalDeploymentNotFoundCode, Message: "deployment is not owned by this plugin"})
		}
		return a.writeInspectionError(c, err)
	}
	if !receipt.ProjectUUID.IsSet() || !receipt.EnvironmentUUID.IsSet() || receipt.ProjectUUID.Must() != projectUUID || receipt.EnvironmentUUID.Must() != environmentUUID ||
		receipt.ProjectID != deployment.ProjectId || receipt.EnvID != deployment.EnvId {
		return c.JSON(http.StatusNotFound, externalDeploymentErrorResponse{Error: externalDeploymentNotFoundCode, Message: "deployment was not found in the authorized Environment"})
	}
	resolved, exactErr := a.resolveExactEnvironment(request.Context(), orgID, externalEnvironmentRef{
		ProjectID: deployment.ProjectId, EnvID: deployment.EnvId, ProjectUUID: projectUUID, EnvironmentUUID: environmentUUID,
	})
	if exactErr != nil || resolved == nil {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: externalEnvironmentIdentityChangedCode, Message: externalEnvironmentIdentityChangedMessage})
	}
	response, err := a.projectExternalDeploymentStatus(request.Context(), tx, *deployment, *receipt, projectUUID, environmentUUID)
	if err != nil {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "deployment_metadata_incomplete", Message: err.Error()})
	}
	if err := tx.Commit(); err != nil {
		return a.writeInspectionError(c, err)
	}
	return c.JSON(http.StatusOK, response)
}

func (a *ExternalDeploymentHTTPAdapter) HandleDispatchRead(c echo.Context) error {
	requestContext, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	c.SetRequest(c.Request().WithContext(requestContext))
	if _, err := a.verifyInspectionGET(c, ExternalDeploymentReadAction); err != nil {
		return err
	}
	dispatchID := c.Param("dispatchId")
	projectUUID, projectErr := uuid.Parse(c.Param("projectUUID"))
	environmentUUID, environmentErr := uuid.Parse(c.Param("environmentUUID"))
	if dispatchID == "" || len(dispatchID) > 128 || projectErr != nil || environmentErr != nil || projectUUID == uuid.Nil || environmentUUID == uuid.Nil {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "dispatch and Environment identity is invalid"})
	}
	// This transaction deliberately takes the same durable dispatch-key lock as
	// admission. After Core revokes a dispatch, the read therefore cannot report
	// absence until any callback that already crossed admission has committed or
	// rolled back. Lock order is DP dispatch key, then the Core authorization
	// callback; Core must commit revocation before starting this read.
	tx, err := a.Admission.Database.BeginTx(c.Request().Context(), nil)
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	defer func() { _ = tx.Rollback() }()
	receipt, err := model.GetExternalOperationDeploymentReceipt(c.Request().Context(), tx, model.ExternalOperationDeploymentKey{
		OrgID: c.Param("orgId"), PluginID: c.Param("pluginId"), RequestID: dispatchID,
	})
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return c.JSON(http.StatusNotFound, externalDeploymentErrorResponse{Error: "dispatch_not_found", Message: "external deployment dispatch was not found"})
		}
		return a.writeInspectionError(c, err)
	}
	if !receipt.ProjectUUID.IsSet() || !receipt.EnvironmentUUID.IsSet() || receipt.ProjectUUID.Must() != projectUUID || receipt.EnvironmentUUID.Must() != environmentUUID {
		return c.JSON(http.StatusNotFound, externalDeploymentErrorResponse{Error: "dispatch_not_found", Message: "external deployment dispatch was not found in the authorized Environment"})
	}
	deployment, _, _, _, err := a.Admission.Database.GetDeployment(c.Request().Context(), tx, c.Param("orgId"), receipt.DeploymentID, model.GetModeDefault)
	if err != nil || deployment.DeploymentEnvUuid != environmentUUID || deployment.ProjectId != receipt.ProjectID || deployment.EnvId != receipt.EnvID {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "dispatch_target_unavailable", Message: "dispatch target is no longer retained with verifiable ownership"})
	}
	if _, err := a.resolveExactEnvironment(c.Request().Context(), c.Param("orgId"), externalEnvironmentRef{
		ProjectID: receipt.ProjectID, EnvID: receipt.EnvID, ProjectUUID: projectUUID, EnvironmentUUID: environmentUUID,
	}); err != nil {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: externalEnvironmentIdentityChangedCode, Message: externalEnvironmentIdentityChangedMessage})
	}
	response, err := a.projectExternalDeploymentStatus(c.Request().Context(), tx, *deployment, *receipt, projectUUID, environmentUUID)
	if err != nil {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "deployment_metadata_incomplete", Message: err.Error()})
	}
	if err := tx.Commit(); err != nil {
		return a.writeInspectionError(c, err)
	}
	return c.JSON(http.StatusOK, response)
}

func (a *ExternalDeploymentHTTPAdapter) verifyInspectionGET(c echo.Context, action string) (plugincontext.Claims, error) {
	if err := a.validateConfiguration(); err != nil {
		return plugincontext.Claims{}, c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentUnavailableCode, Message: "external deployment read is unavailable"})
	}
	request := c.Request()
	if request.URL.RawQuery != "" || request.URL.Fragment != "" {
		return plugincontext.Claims{}, c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: externalQueryUnsupportedMessage})
	}
	assertion, ok := singleHeader(request.Header, plugincontext.Header)
	if !ok || assertion == "" {
		return plugincontext.Claims{}, c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidPluginContextMessage})
	}
	orgID, pluginID := c.Param("orgId"), c.Param("pluginId")
	if !validExternalPathIdentity(orgID, pluginID) {
		return plugincontext.Claims{}, c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: externalInvalidPathIdentityMessage})
	}
	claims, err := plugincontext.Verify(a.VerificationKey, assertion, plugincontext.Request{
		Issuer: a.Issuer, Audience: ExternalDeploymentAudience, PluginID: pluginID, OrgID: orgID,
		Method: http.MethodGet, Path: request.URL.EscapedPath(), Body: nil, OperationID: action,
	}, a.now())
	if err != nil {
		return plugincontext.Claims{}, c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidPluginContextMessage})
	}
	if _, err := uuid.Parse(claims.ActorID); err != nil {
		return plugincontext.Claims{}, c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidActorMessage})
	}
	return claims, nil
}

func (a *ExternalDeploymentHTTPAdapter) projectExternalDeploymentStatus(ctx context.Context, tx model.Tx, deployment model.DeploymentSummary, receipt model.ExternalOperationDeploymentReceipt, projectUUID, environmentUUID uuid.UUID) (externalDeploymentStatusResponse, error) {
	versions, err := immutableModuleVersions(deployment.ModuleArtifacts)
	if err != nil {
		return externalDeploymentStatusResponse{}, err
	}
	if !receipt.BaseDeploymentID.IsSet() {
		return externalDeploymentStatusResponse{}, errors.New("external deployment receipt lacks a captured effective base")
	}
	base, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, receipt.OrgID, receipt.BaseDeploymentID.Must(), model.GetModeDefault)
	if err != nil || base.DeploymentEnvUuid != environmentUUID || base.ProjectId != deployment.ProjectId || base.EnvId != deployment.EnvId {
		return externalDeploymentStatusResponse{}, errors.New("captured effective base is not retained with verifiable ownership")
	}
	rollbackVersions, err := immutableModuleVersions(base.ModuleArtifacts)
	if err != nil {
		return externalDeploymentStatusResponse{}, err
	}
	return externalDeploymentStatusResponse{
		DeploymentID: deployment.Id, DispatchID: receipt.RequestID, OperationRef: receipt.OperationRef, Fence: receipt.Fence,
		RequestDigest: receipt.RequestBodySHA256,
		ProjectID:     deployment.ProjectId, EnvID: deployment.EnvId, ProjectUUID: projectUUID, EnvironmentUUID: environmentUUID,
		Mode: string(deployment.Mode), Status: string(deployment.Status), StatusMessage: deployment.StatusMessage,
		ModuleVersions: versions, RollbackModuleVersions: rollbackVersions,
		CreatedAt: deployment.CreatedAt, CompletedAt: deployment.CompletedAt.Ref(), ReceiptCreatedAt: receipt.CreatedAt,
	}, nil
}

func (a *ExternalDeploymentHTTPAdapter) HandleReadiness(c echo.Context) error {
	requestContext, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	c.SetRequest(c.Request().WithContext(requestContext))
	bodyBytes, _, err := a.verifyInspectionRequest(c, ExternalDeploymentReadinessAction)
	if err != nil {
		return err
	}
	var body externalDeploymentReadinessBody
	if err := decodeStrictJSON(bodyBytes, &body); err != nil || len(body.Environments) == 0 || len(body.Environments) > maxExternalReadinessEnvironments {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "environments must contain between 1 and 1000 exact references"})
	}
	refs := make([]model.EnvironmentRef, len(body.Environments))
	seen := make(map[string]struct{}, len(refs))
	for index, ref := range body.Environments {
		if ref.ProjectID == "" || ref.EnvID == "" || ref.ProjectUUID == uuid.Nil || ref.EnvironmentUUID == uuid.Nil {
			return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "project and Environment slugs and UUIDs are required"})
		}
		key := ref.ProjectID + "\x00" + ref.EnvID
		if _, duplicate := seen[key]; duplicate {
			return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "environment references must be unique"})
		}
		seen[key] = struct{}{}
		if _, err := a.resolveExactEnvironment(c.Request().Context(), c.Param("orgId"), ref); err != nil {
			return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: externalEnvironmentIdentityChangedCode, Message: "an authorized Environment identity is no longer current"})
		}
		refs[index] = model.EnvironmentRef{ProjectID: ref.ProjectID, EnvID: ref.EnvID}
	}
	tx, err := a.Admission.Database.BeginTx(c.Request().Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	defer func() { _ = tx.Rollback() }()
	items, err := a.Admission.Database.GetEnvironmentDeploymentReadiness(c.Request().Context(), tx, c.Param("orgId"), refs)
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	if err := tx.Commit(); err != nil {
		return a.writeInspectionError(c, err)
	}
	response := externalDeploymentReadinessResponse{Items: make([]externalEnvironmentReadiness, len(items))}
	for index := range items {
		if !items[index].EnvironmentUUID.IsSet() || items[index].EnvironmentUUID.Must() != body.Environments[index].EnvironmentUUID {
			return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: externalEnvironmentIdentityChangedCode, Message: "deployment history belongs to a different Environment identity"})
		}
		response.Items[index] = projectEnvironmentReadiness(items[index])
		response.Items[index].ProjectUUID = body.Environments[index].ProjectUUID
	}
	return c.JSON(http.StatusOK, response)
}

func (a *ExternalDeploymentHTTPAdapter) HandlePlan(c echo.Context) error {
	requestContext, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	c.SetRequest(c.Request().WithContext(requestContext))
	bodyBytes, claims, err := a.verifyInspectionRequest(c, ExternalDeploymentPlanningAction)
	if err != nil {
		return err
	}
	var body externalDeploymentPlanBody
	if err := decodeStrictJSON(bodyBytes, &body); err != nil || body.ProjectID == "" || body.EnvID == "" || body.ProjectUUID == uuid.Nil || body.EnvironmentUUID == uuid.Nil || len(body.ModuleVersions) == 0 {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "project and Environment identity and module_versions are required"})
	}
	explicitVersions := make(map[string]string, len(body.ModuleVersions))
	explicitTargets := make([]model.ExternalDeploymentPlanTarget, 0, len(body.ModuleVersions))
	for moduleID, selected := range body.ModuleVersions {
		if moduleID == "" || selected.Version == "" || selected.ModuleUUID == uuid.Nil || selected.VersionUUID == uuid.Nil || selected.ArtifactDigest == "" {
			return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "module selections require immutable module and version identity"})
		}
		explicitVersions[moduleID] = selected.Version
		explicitTargets = append(explicitTargets, model.ExternalDeploymentPlanTarget{
			ModuleID: moduleID, ModuleUUID: selected.ModuleUUID, Version: selected.Version,
			VersionUUID: selected.VersionUUID, SemanticVersion: selected.SemanticVersion, ArtifactDigest: selected.ArtifactDigest,
		})
	}
	slices.SortFunc(explicitTargets, func(left, right model.ExternalDeploymentPlanTarget) int {
		return strings.Compare(left.ModuleID, right.ModuleID)
	})
	actorID := uuid.MustParse(claims.ActorID)
	tx, err := a.Admission.Database.BeginTx(c.Request().Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	defer func() { _ = tx.Rollback() }()
	if stored, lookupErr := model.GetExternalDeploymentPlanSnapshot(c.Request().Context(), tx, c.Param("orgId"), c.Param("pluginId"), claims.RequestID); lookupErr == nil {
		if stored.ActorID != actorID || stored.RequestBodySHA256 != claims.BodySHA256 || stored.ProjectID != body.ProjectID || stored.ProjectUUID != body.ProjectUUID || stored.EnvID != body.EnvID || stored.EnvironmentUUID != body.EnvironmentUUID {
			return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "planning_request_conflict", Message: "planning request identity was reused with different authority or scope"})
		}
		if err := tx.Commit(); err != nil {
			return a.writeInspectionError(c, err)
		}
		response, err := externalPlanResponse(*stored)
		if err != nil {
			return a.writeInspectionError(c, err)
		}
		return c.JSON(http.StatusOK, response)
	} else if _, notFound := model.IsErrNotFound(lookupErr); !notFound {
		return a.writeInspectionError(c, lookupErr)
	}

	readiness, err := a.Admission.Database.GetEnvironmentDeploymentReadiness(c.Request().Context(), tx, c.Param("orgId"), []model.EnvironmentRef{{ProjectID: body.ProjectID, EnvID: body.EnvID}})
	if err != nil || len(readiness) != 1 {
		return a.writeInspectionError(c, err)
	}
	if projectEnvironmentReadiness(readiness[0]).State != "ready" {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "environment_not_ready", Message: "environment does not have a reconciled planning base"})
	}
	environment, err := a.resolveExactEnvironment(c.Request().Context(), c.Param("orgId"), externalEnvironmentRef{
		ProjectID: body.ProjectID, EnvID: body.EnvID, ProjectUUID: body.ProjectUUID, EnvironmentUUID: body.EnvironmentUUID,
	})
	if err != nil || environment == nil || !readiness[0].EnvironmentUUID.IsSet() || environment.Uuid != readiness[0].EnvironmentUUID.Must() {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: externalEnvironmentIdentityChangedCode, Message: "target environment identity does not match its deployment history"})
	}

	capture := &DeploymentPlanCapture{}
	_, _, diff, err := CreateDeploymentWithOptions(
		c.Request().Context(), a.logger(), actorID, c.Param("orgId"), body.ProjectID, body.EnvID,
		*environment, model.DeploymentModeDeploy, opt.Empty[uuid.UUID](), DeploymentManifest{}, nil, nil, nil, true, nil,
		a.ControlPlaneClient, a.Admission.Database, tx, DeploymentOptions{
			ExplicitModuleVersions: explicitVersions, ReuseLastSuccessfulManifest: true, IgnoreEnvironmentPins: true, CapturePlan: capture,
			AuthorizeModuleVersion: func(moduleID string, explicitlySelected bool, artifact catalogueArtifact) error {
				if !explicitlySelected {
					return nil
				}
				selected := body.ModuleVersions[moduleID]
				if artifact.ModuleUUID != selected.ModuleUUID || artifact.VersionUUID != selected.VersionUUID ||
					artifact.SemanticVersion != selected.SemanticVersion || artifact.Digest != selected.ArtifactDigest {
					return model.NewErrConflict("selected module version identity changed during planning")
				}
				return nil
			},
		},
	)
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	if capture.BaseDeploymentID != readiness[0].EffectiveBaseDeploymentID.Must() || diff == nil {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "planning_base_changed", Message: "environment planning base changed during planning"})
	}
	for moduleID, selected := range body.ModuleVersions {
		matched := false
		for _, artifact := range capture.ModuleArtifacts {
			if artifact.ModuleID == moduleID && artifact.ModuleUUID == selected.ModuleUUID && artifact.Version == selected.Version && artifact.VersionUUID == selected.VersionUUID && artifact.ArtifactDigest == selected.ArtifactDigest {
				matched = true
				break
			}
		}
		if !matched {
			return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "module_selection_not_active", Message: "selected module version is not active in the planned graph"})
		}
	}
	fingerprint := deploymentPlanFingerprint(*capture)
	stored, err := model.CreateOrGetExternalDeploymentPlanSnapshot(c.Request().Context(), tx, model.ExternalDeploymentPlanSnapshot{
		OrgID: c.Param("orgId"), PluginID: c.Param("pluginId"), RequestID: claims.RequestID, ActorID: actorID,
		RequestBodySHA256: claims.BodySHA256, ProjectID: body.ProjectID, ProjectUUID: body.ProjectUUID, EnvID: body.EnvID, EnvironmentUUID: environment.Uuid,
		BaseDeploymentID: capture.BaseDeploymentID, Fingerprint: fingerprint, Manifest: capture.Manifest, Graph: capture.Graph,
		Diff: capture.Diff, ModuleArtifacts: capture.ModuleArtifacts, ExplicitTargets: explicitTargets,
	})
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	if err := tx.Commit(); err != nil {
		return a.writeInspectionError(c, err)
	}
	response, err := externalPlanResponse(*stored)
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	return c.JSON(http.StatusOK, response)
}

func (a *ExternalDeploymentHTTPAdapter) resolveExactEnvironment(ctx context.Context, orgID string, ref externalEnvironmentRef) (*platformorchestratorcp.Environment, error) {
	response, err := a.ControlPlaneClient.GetEnvironmentWithResponse(ctx, orgID, ref.ProjectID, ref.EnvID)
	if err != nil || response == nil || response.StatusCode() != http.StatusOK || response.JSON200 == nil || response.JSON200.ProjectUuid == nil {
		return nil, errors.New("core did not return an authoritative environment identity")
	}
	environment := response.JSON200
	if environment.ProjectId != ref.ProjectID || environment.Id != ref.EnvID || *environment.ProjectUuid != ref.ProjectUUID || environment.Uuid != ref.EnvironmentUUID {
		return nil, errors.New("core environment identity mismatch")
	}
	return environment, nil
}

func externalPlanResponse(snapshot model.ExternalDeploymentPlanSnapshot) (externalDeploymentPlanResponse, error) {
	manifest, graph, diff, err := projectPlanCapture(DeploymentPlanCapture{
		BaseDeploymentID: snapshot.BaseDeploymentID, Manifest: snapshot.Manifest, Graph: snapshot.Graph,
		Diff: snapshot.Diff, ModuleArtifacts: snapshot.ModuleArtifacts,
	})
	if err != nil {
		return externalDeploymentPlanResponse{}, err
	}
	return externalDeploymentPlanResponse{
		SnapshotID: snapshot.ID, EnvironmentUUID: snapshot.EnvironmentUUID, EffectiveBaseDeploymentID: snapshot.BaseDeploymentID,
		Fingerprint: snapshot.Fingerprint, Manifest: manifest, Graph: graph, Diff: diff,
		ModuleVersions: projectModuleInventory(snapshot.ModuleArtifacts),
	}, nil
}

func (a *ExternalDeploymentHTTPAdapter) verifyInspectionRequest(c echo.Context, action string) ([]byte, plugincontext.Claims, error) {
	if err := a.validateConfiguration(); err != nil {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentUnavailableCode, Message: "external deployment inspection is unavailable"})
	}
	request := c.Request()
	if request.URL.RawQuery != "" || request.URL.Fragment != "" {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: externalQueryUnsupportedMessage})
	}
	if contentType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type")); err != nil || contentType != "application/json" {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusUnsupportedMediaType, externalDeploymentErrorResponse{Error: "invalid_content_type", Message: "Content-Type must be application/json"})
	}
	idempotencyKey, ok := singleHeader(request.Header, "Idempotency-Key")
	if !ok || idempotencyKey == "" || len(idempotencyKey) > 128 {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: "invalid_idempotency_key", Message: "one request identifier of at most 128 characters is required"})
	}
	assertion, ok := singleHeader(request.Header, plugincontext.Header)
	if !ok || assertion == "" {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidPluginContextMessage})
	}
	request.Body = http.MaxBytesReader(c.Response(), request.Body, maxExternalInspectionBodyBytes)
	bodyBytes, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "request body is invalid or too large"})
	}
	orgID, pluginID := c.Param("orgId"), c.Param("pluginId")
	if !validExternalPathIdentity(orgID, pluginID) {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: externalInvalidPathIdentityMessage})
	}
	claims, err := plugincontext.Verify(a.VerificationKey, assertion, plugincontext.Request{
		Issuer: a.Issuer, Audience: ExternalDeploymentAudience, PluginID: pluginID, OrgID: orgID,
		Method: http.MethodPost, Path: request.URL.EscapedPath(), Body: bodyBytes,
		OperationID: action, IdempotencyKey: idempotencyKey,
	}, a.now())
	if err != nil {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidPluginContextMessage})
	}
	if _, err := uuid.Parse(claims.ActorID); err != nil {
		return nil, plugincontext.Claims{}, c.JSON(http.StatusUnauthorized, externalDeploymentErrorResponse{Error: externalInvalidPluginContextCode, Message: externalInvalidActorMessage})
	}
	return bodyBytes, claims, nil
}

func (a *ExternalDeploymentHTTPAdapter) writeInspectionError(c echo.Context, err error) error {
	if err == nil {
		return c.JSON(http.StatusInternalServerError, externalDeploymentErrorResponse{Error: externalDeploymentFailedCode, Message: "external deployment inspection failed"})
	}
	if badRequest, ok := model.IsErrBadRequest(err); ok {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: badRequest.Message})
	}
	if conflict, ok := model.IsErrConflict(err); ok {
		return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "planning_conflict", Message: conflict.Message})
	}
	a.logger().Error("external deployment inspection failed")
	return c.JSON(http.StatusInternalServerError, externalDeploymentErrorResponse{Error: externalDeploymentFailedCode, Message: "external deployment inspection failed"})
}

func decodeStrictJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func projectEnvironmentReadiness(item model.EnvironmentDeploymentReadiness) externalEnvironmentReadiness {
	state := "unreconciled"
	if item.LatestStatus.IsSet() {
		switch item.LatestStatus.Must() {
		case model.DeploymentStatusExecuting:
			if item.ActiveLeaseUntil.IsSet() {
				state = "busy"
			}
		case model.DeploymentStatusSucceeded:
			state = "ready"
		}
	}
	result := externalEnvironmentReadiness{
		ProjectID: item.ProjectID, EnvID: item.EnvID, State: state,
		EnvironmentUUID: item.EnvironmentUUID.Ref(), LatestDeploymentID: item.LatestDeploymentID.Ref(),
		EffectiveBaseDeploymentID: item.EffectiveBaseDeploymentID.Ref(), ActiveModules: projectModuleInventory(item.ActiveModules),
	}
	if item.LatestStatus.IsSet() {
		result.LatestStatus = string(item.LatestStatus.Must())
	}
	if item.LatestMode.IsSet() {
		result.LatestMode = string(item.LatestMode.Must())
	}
	if item.ActiveLeaseUntil.IsSet() {
		result.ActiveLeaseUntil = item.ActiveLeaseUntil.Ref()
	}
	return result
}

func projectModuleInventory(artifacts []model.ModuleArtifactRequirement) []externalModuleInventory {
	result := make([]externalModuleInventory, len(artifacts))
	for index, artifact := range artifacts {
		result[index] = externalModuleInventory{
			ModuleID: artifact.ModuleID, Version: artifact.Version, SemanticVersion: artifact.SemanticVersion, ArtifactDigest: artifact.ArtifactDigest,
		}
		if artifact.ModuleUUID != uuid.Nil {
			result[index].ModuleUUID = &artifact.ModuleUUID
		}
		if artifact.VersionUUID != uuid.Nil {
			result[index].VersionUUID = &artifact.VersionUUID
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ModuleID+"@"+result[i].Version < result[j].ModuleID+"@"+result[j].Version
	})
	return result
}

func projectPlanCapture(capture DeploymentPlanCapture) (externalManifestStructure, externalGraphStructure, DeploymentDiff, error) {
	var manifest DeploymentManifest
	if err := json.Unmarshal(capture.Manifest, &manifest); err != nil {
		return externalManifestStructure{}, externalGraphStructure{}, DeploymentDiff{}, err
	}
	graph, err := graphs.FromJson(bytes.NewReader(capture.Graph))
	if err != nil {
		return externalManifestStructure{}, externalGraphStructure{}, DeploymentDiff{}, err
	}
	var diff DeploymentDiff
	if err := json.Unmarshal(capture.Diff, &diff); err != nil {
		return externalManifestStructure{}, externalGraphStructure{}, DeploymentDiff{}, err
	}
	manifestProjection := externalManifestStructure{Shared: projectManifestResources(manifest.Shared), Workloads: make(map[string]externalManifestWorkloadStructure, len(manifest.Workloads))}
	for name, workload := range manifest.Workloads {
		manifestProjection.Workloads[name] = externalManifestWorkloadStructure{OutputKeys: sortedMapKeys(workload.Outputs), Resources: projectManifestResources(workload.Resources)}
	}
	graphProjection := externalGraphStructure{
		Nodes: make(map[string]externalGraphNodeStructure, len(graph.Nodes)), Edges: make(map[string]map[string]string, len(graph.Edges)),
		Workloads: make(map[string]string, len(graph.Workloads)), SharedResources: make(map[string]string, len(graph.SharedResources)),
	}
	for coordinate, node := range graph.Nodes {
		projected := externalGraphNodeStructure{ParamKeys: sortedMapKeys(node.Params), Drift: string(node.EffectiveDrift)}
		if node.ModuleConfiguration != nil {
			projected.ModuleID = node.ModuleConfiguration.DefinitionId
			projected.Version = node.ModuleConfiguration.VersionId
			projected.Deleted = node.ModuleConfiguration.Deleted
		}
		graphProjection.Nodes[coordinate.String()] = projected
	}
	for from, edges := range graph.Edges {
		projected := make(map[string]string, len(edges))
		for label, to := range edges {
			projected[label] = to.String()
		}
		graphProjection.Edges[from.String()] = projected
	}
	for name, coordinate := range graph.Workloads {
		graphProjection.Workloads[name] = coordinate.String()
	}
	for name, coordinate := range graph.SharedResources {
		graphProjection.SharedResources[name] = coordinate.String()
	}
	for coordinate, leaf := range graph.LeafResources {
		if leaf {
			graphProjection.LeafResources = append(graphProjection.LeafResources, coordinate.String())
		}
	}
	slices.Sort(graphProjection.LeafResources)
	return manifestProjection, graphProjection, diff, nil
}

func projectManifestResources(resources map[string]DeploymentManifestResource) map[string]externalManifestResourceStructure {
	result := make(map[string]externalManifestResourceStructure, len(resources))
	for name, resource := range resources {
		result[name] = externalManifestResourceStructure{Type: resource.Type, Class: (*string)(resource.Class), ID: (*string)(resource.Id), ParamKeys: sortedMapKeys(resource.Params)}
	}
	return result
}

func sortedMapKeys[M ~map[string]V, V any](values M) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

func deploymentPlanFingerprint(capture DeploymentPlanCapture) string {
	hash := sha256.New()
	writeFingerprintChunk := func(value []byte) {
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(value)
	}
	writeFingerprintChunk([]byte(capture.BaseDeploymentID.String()))
	writeFingerprintChunk(canonicalFingerprintJSON(capture.Manifest))
	writeFingerprintChunk(canonicalFingerprintJSON(capture.Graph))
	writeFingerprintChunk(canonicalFingerprintJSON(capture.Diff))
	artifacts, _ := json.Marshal(capture.ModuleArtifacts)
	writeFingerprintChunk(artifacts)
	return "sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func canonicalFingerprintJSON(raw []byte) []byte {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return raw
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return raw
	}
	return canonical
}
