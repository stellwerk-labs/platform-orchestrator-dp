package api

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const (
	ExternalDeploymentLogsPath      = "/orgs/:orgId/plugins/:pluginId/projects/:projectUUID/envs/:environmentUUID/external-deployments/:deploymentId/logs"
	ExternalDeploymentOutputsPath   = "/orgs/:orgId/plugins/:pluginId/projects/:projectUUID/envs/:environmentUUID/external-deployments/:deploymentId/outputs"
	ExternalDeploymentLogsAction    = "external-deployment.logs.read"
	ExternalDeploymentOutputsAction = "external-deployment.outputs.read"
	maxExternalMaterialBytes        = 16 << 20
	maxExternalMaterialEncodedBytes = 24 << 20
)

var (
	errExternalMaterialInvalid  = errors.New("external deployment material is invalid")
	errExternalMaterialTooLarge = errors.New("external deployment material exceeds the response limit")
)

type externalDeploymentMaterialKind string

const (
	externalDeploymentLogs    externalDeploymentMaterialKind = "logs"
	externalDeploymentOutputs externalDeploymentMaterialKind = "outputs"
)

func (a *ExternalDeploymentHTTPAdapter) HandleLogs(c echo.Context) error {
	return a.handleMaterial(c, externalDeploymentLogs, ExternalDeploymentLogsAction)
}

func (a *ExternalDeploymentHTTPAdapter) HandleOutputs(c echo.Context) error {
	return a.handleMaterial(c, externalDeploymentOutputs, ExternalDeploymentOutputsAction)
}

func (a *ExternalDeploymentHTTPAdapter) handleMaterial(c echo.Context, kind externalDeploymentMaterialKind, action string) error {
	if err := a.validateMaterialConfiguration(kind); err != nil {
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentUnavailableCode, Message: "external deployment material is unavailable"})
	}
	requestContext, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	c.SetRequest(c.Request().WithContext(requestContext))
	if _, err := a.verifyInspectionGET(c, action); err != nil {
		return err
	}

	deploymentID, deploymentErr := uuid.Parse(c.Param("deploymentId"))
	projectUUID, projectErr := uuid.Parse(c.Param("projectUUID"))
	environmentUUID, environmentErr := uuid.Parse(c.Param("environmentUUID"))
	if deploymentErr != nil || projectErr != nil || environmentErr != nil || deploymentID == uuid.Nil || projectUUID == uuid.Nil || environmentUUID == uuid.Nil {
		return c.JSON(http.StatusBadRequest, externalDeploymentErrorResponse{Error: externalDeploymentInvalidRequestCode, Message: "deployment and Environment identity is invalid"})
	}

	tx, err := a.Admission.Database.BeginTx(c.Request().Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return a.writeInspectionError(c, err)
	}
	defer func() { _ = tx.Rollback() }()
	orgID, pluginID := c.Param("orgId"), c.Param("pluginId")
	deployment, _, _, _, err := a.Admission.Database.GetDeployment(c.Request().Context(), tx, orgID, deploymentID, model.GetModeDefault)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return a.writeMaterialNotFound(c, kind)
		}
		return a.writeInspectionError(c, err)
	}
	receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(c.Request().Context(), tx, orgID, pluginID, deploymentID)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return a.writeMaterialNotFound(c, kind)
		}
		return a.writeInspectionError(c, err)
	}
	if deployment.DeploymentEnvUuid != environmentUUID || !receipt.ProjectUUID.IsSet() || !receipt.EnvironmentUUID.IsSet() ||
		receipt.ProjectUUID.Must() != projectUUID || receipt.EnvironmentUUID.Must() != environmentUUID ||
		receipt.ProjectID != deployment.ProjectId || receipt.EnvID != deployment.EnvId {
		return a.writeMaterialNotFound(c, kind)
	}
	operationRef, err := uuid.Parse(receipt.OperationRef)
	if err != nil || operationRef == uuid.Nil {
		return a.writeMaterialConflict(c, "deployment receipt has invalid operation material identity")
	}
	material, err := model.GetExternalOperationMaterial(c.Request().Context(), tx, orgID, pluginID, operationRef)
	if err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return a.writeMaterialConflict(c, "deployment encryption material is not retained")
		}
		return a.writeInspectionError(c, err)
	}
	materialDeployment := *deployment
	if kind == externalDeploymentOutputs && materialDeployment.Status == model.DeploymentStatusFailed && materialDeployment.CompletedAt.IsSet() {
		late, err := lateSuccessfulOutputs(c.Request().Context(), tx, *deployment)
		if err != nil {
			return a.writeInspectionError(c, err)
		}
		// Local ownership validation may consume late ciphertext, without
		// persisting or exposing a succeeded Deployment projection.
		if late {
			materialDeployment.Status = model.DeploymentStatusSucceeded
		}
	}
	if err := validateExternalDeploymentMaterialOwner(materialDeployment, *material, kind); err != nil {
		if _, missing := model.IsErrNotFound(err); missing {
			return a.writeMaterialNotFound(c, kind)
		}
		return a.writeMaterialConflict(c, err.Error())
	}
	if err := tx.Commit(); err != nil {
		return a.writeInspectionError(c, err)
	}

	identity, err := a.externalOperationIdentity(c.Request().Context(), material.EncryptedIdentity)
	if err != nil {
		if errors.Is(err, errExternalMaterialInvalid) {
			return a.writeMaterialConflict(c, "stored deployment encryption identity is invalid")
		}
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentUnavailableCode, Message: "external deployment decryption material is unavailable"})
	}
	encoded, closeMaterial, err := a.externalDeploymentEncodedMaterial(c.Request().Context(), orgID, deploymentID, *deployment, kind)
	if closeMaterial != nil {
		defer func() { _ = closeMaterial.Close() }()
	}
	if err != nil {
		if errors.Is(err, ErrRunnerLogsNotFound) {
			return a.writeMaterialNotFound(c, kind)
		}
		if _, missing := model.IsErrNotFound(err); missing {
			return a.writeMaterialNotFound(c, kind)
		}
		if errors.Is(err, errExternalMaterialTooLarge) {
			return c.JSON(http.StatusRequestEntityTooLarge, externalDeploymentErrorResponse{Error: "deployment_material_too_large", Message: "external deployment material exceeds the 16 MiB response limit"})
		}
		return c.JSON(http.StatusServiceUnavailable, externalDeploymentErrorResponse{Error: externalDeploymentUnavailableCode, Message: "external deployment material storage is unavailable"})
	}
	payload, err := decryptExternalDeploymentPayload(encoded, identity, maxExternalMaterialBytes)
	if err != nil {
		switch {
		case errors.Is(err, errExternalMaterialTooLarge):
			return c.JSON(http.StatusRequestEntityTooLarge, externalDeploymentErrorResponse{Error: "deployment_material_too_large", Message: "external deployment material exceeds the 16 MiB response limit"})
		default:
			return a.writeMaterialConflict(c, "stored deployment material cannot be decrypted")
		}
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	extension := "log"
	if kind == externalDeploymentOutputs {
		extension = "outputs.json"
	}
	c.Response().Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s.%s\"", deploymentID, extension))
	c.Response().Header().Set("X-Content-Type-Options", "nosniff")
	return c.Blob(http.StatusOK, "text/plain; charset=utf-8", payload)
}

func (a *ExternalDeploymentHTTPAdapter) validateMaterialConfiguration(kind externalDeploymentMaterialKind) error {
	if err := a.validateConfiguration(); err != nil {
		return err
	}
	if a.Materials.Vault == nil || (kind == externalDeploymentLogs && a.RunnerLogsReader == nil) {
		return errors.New("external deployment material adapter is not configured")
	}
	return nil
}

func validateExternalDeploymentMaterialOwner(deployment model.DeploymentSummary, material model.ExternalOperationMaterial, kind externalDeploymentMaterialKind) error {
	var recipient string
	switch kind {
	case externalDeploymentLogs:
		if !deployment.EncryptedLogsRecipient.IsSet() {
			return errors.New("deployment has no retained encrypted logs recipient")
		}
		recipient = deployment.EncryptedLogsRecipient.Must()
	case externalDeploymentOutputs:
		if deployment.Mode == model.DeploymentModeDestroy || !deployment.CompletedAt.IsSet() || deployment.Status != model.DeploymentStatusSucceeded {
			return model.NewErrNotFound("deployment outputs are not available")
		}
		if !deployment.EncryptedOutputsRecipient.IsSet() {
			return errors.New("deployment has no retained encrypted outputs recipient")
		}
		recipient = deployment.EncryptedOutputsRecipient.Must()
	default:
		return errors.New("unsupported external deployment material kind")
	}
	if recipient != material.Recipient {
		return errors.New("deployment encryption recipient differs from its operation material")
	}
	return nil
}

func (a *ExternalDeploymentHTTPAdapter) externalOperationIdentity(ctx context.Context, encryptedIdentity string) (age.Identity, error) {
	plaintext, err := a.Materials.Vault.Decrypt(ctx, externalOperationMaterialTransitKey, encryptedIdentity)
	if err != nil {
		return nil, err
	}
	defer clear(plaintext)
	identity, err := age.ParseX25519Identity(string(plaintext))
	if err != nil {
		return nil, errExternalMaterialInvalid
	}
	return identity, nil
}

func (a *ExternalDeploymentHTTPAdapter) externalDeploymentEncodedMaterial(
	ctx context.Context, orgID string, deploymentID uuid.UUID, deployment model.DeploymentSummary, kind externalDeploymentMaterialKind,
) (io.Reader, io.Closer, error) {
	switch kind {
	case externalDeploymentLogs:
		reader, err := a.RunnerLogsReader(ctx, deployment.DeploymentEnvUuid.String()+"/"+deploymentID.String())
		return reader, reader, err
	case externalDeploymentOutputs:
		encoded, err := a.Admission.Database.GetDeploymentEncryptedOutputs(ctx, nil, orgID, deploymentID)
		if err != nil {
			return nil, nil, err
		}
		if encoded == "" {
			return nil, nil, model.NewErrNotFound("deployment outputs are no longer available")
		}
		if len(encoded) > maxExternalMaterialEncodedBytes {
			return nil, nil, errExternalMaterialTooLarge
		}
		return strings.NewReader(encoded), nil, nil
	default:
		return nil, nil, errExternalMaterialInvalid
	}
}

func decryptExternalDeploymentPayload(encoded io.Reader, identity age.Identity, limit int64) ([]byte, error) {
	if encoded == nil || identity == nil || limit <= 0 {
		return nil, errExternalMaterialInvalid
	}
	decoded := base64.NewDecoder(base64.StdEncoding, io.LimitReader(encoded, maxExternalMaterialEncodedBytes+1))
	decrypted, err := age.Decrypt(decoded, identity)
	if err != nil {
		return nil, errors.Join(errExternalMaterialInvalid, err)
	}
	limited := &io.LimitedReader{R: decrypted, N: limit + 1}
	payload, err := io.ReadAll(limited)
	if err != nil {
		return nil, errors.Join(errExternalMaterialInvalid, err)
	}
	if int64(len(payload)) > limit {
		return nil, errExternalMaterialTooLarge
	}
	return payload, nil
}

func (a *ExternalDeploymentHTTPAdapter) writeMaterialNotFound(c echo.Context, kind externalDeploymentMaterialKind) error {
	return c.JSON(http.StatusNotFound, externalDeploymentErrorResponse{Error: "deployment_material_not_found", Message: "external deployment " + string(kind) + " were not found"})
}

func (a *ExternalDeploymentHTTPAdapter) writeMaterialConflict(c echo.Context, message string) error {
	return c.JSON(http.StatusConflict, externalDeploymentErrorResponse{Error: "deployment_material_invalid", Message: message})
}
