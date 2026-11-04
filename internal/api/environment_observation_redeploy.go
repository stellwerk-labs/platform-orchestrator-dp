package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genevents"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
)

const ObservationRedeployPath = "/orgs/:orgId/plugins/:pluginId/observations/v1/runs/:runId/redeploy"
const ObservationRedeployAction = "observation.redeploy"

func (a *ExternalDeploymentHTTPAdapter) HandleObservationRedeploy(c echo.Context) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	c.SetRequest(c.Request().WithContext(ctx))
	if a.OrdinaryAdmissions == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "ordinary deployment admission required")
	}
	kind, _, err := mime.ParseMediaType(c.Request().Header.Get("Content-Type"))
	if err != nil || kind != observationJSONContentType {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType, "Content-Type must be application/json")
	}
	raw, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxObservationBodyBytes))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "redeploy body exceeds bound")
	}
	key, ok := singleHeader(c.Request().Header, "Idempotency-Key")
	if !ok {
		return echo.NewHTTPError(http.StatusBadRequest, "Idempotency-Key required")
	}
	claims, err := a.authenticateObservation(c, ObservationRedeployAction, raw, key)
	if err != nil {
		return err
	}
	runID, err := uuid.Parse(c.Param("runId"))
	if err != nil || runID == uuid.Nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid observation run")
	}
	var body observation.ObservationRedeployRequest
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil || requireJSONEOF(decoder) != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "strict redeploy request required")
	}
	if body.RequestId == uuid.Nil || body.RequestId.String() != key || body.ProjectUuid == uuid.Nil || body.EnvironmentUuid == uuid.Nil || body.BaselineId == uuid.Nil || !externalPlanFingerprintPattern.MatchString(body.BaselineFingerprint) || body.ProjectId == "" || body.EnvId == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid exact baseline identity")
	}
	actor, _ := uuid.Parse(claims.ActorID)
	digest, err := model.ObservationRequestDigest(body)
	if err != nil {
		return err
	}
	tx, err := a.Admission.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := model.LockObservationRequest(ctx, tx, model.ObservationRequestKindRedeploy, claims.OrgID, claims.PluginID, body.RequestId); err != nil {
		return err
	}
	if err := model.RequireObservationRequestUnsealed(ctx, tx, model.ObservationRequestKindRedeploy, claims.OrgID, claims.PluginID, body.RequestId, actor, runID, digest); err != nil {
		return writeObservationError(c, err)
	}
	var previousRun, previousActor, deploymentID, baselineID uuid.UUID
	var previousDigest string
	err = tx.QueryRowContext(ctx, `SELECT run_id,actor_id,request_sha256,deployment_id,baseline_id FROM environment_observation_redeploy_receipts WHERE org_id=$1 AND plugin_id=$2 AND request_id=$3`, claims.OrgID, claims.PluginID, body.RequestId).
		Scan(&previousRun, &previousActor, &previousDigest, &deploymentID, &baselineID)
	if err == nil {
		if previousRun != runID || previousActor != actor || previousDigest != digest {
			return writeObservationError(c, model.NewErrConflict("redeploy request identity already used with different content"))
		}
		dep, _, _, _, err := a.Admission.Database.GetDeployment(ctx, tx, claims.OrgID, deploymentID, model.GetModeDefault)
		if err != nil {
			return writeObservationError(c, err)
		}
		return c.JSON(http.StatusOK, observation.ObservationRedeployReceipt{RequestId: body.RequestId, DeploymentId: deploymentID, BaselineId: baselineID, Status: observation.ObservationRedeployReceiptStatus(dep.Status)})
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := model.LockEnvironmentMutation(ctx, tx, body.EnvironmentUuid); err != nil {
		return err
	}
	run, err := model.ReadObservationRun(ctx, tx, claims.OrgID, claims.PluginID, runID)
	if err != nil {
		return writeObservationError(c, err)
	}
	if run.Receipt.BaselineId == nil || run.Receipt.BaselineFingerprint == nil || *run.Receipt.BaselineId != body.BaselineId || *run.Receipt.BaselineFingerprint != body.BaselineFingerprint {
		return writeObservationError(c, model.NewErrConflict("finding baseline identity changed"))
	}
	if err := model.CheckEnvironmentAvailable(ctx, tx, claims.OrgID, body.ProjectId, body.EnvId, body.EnvironmentUuid, uuid.Nil); err != nil {
		return writeObservationError(c, err)
	}
	head, _, _, _, err := a.Admission.Database.GetLastDeployment(ctx, tx, claims.OrgID, body.ProjectId, body.EnvId, model.GetLastDeploymentParams{StateChangeOnly: true})
	if err != nil {
		return writeObservationError(c, err)
	}
	base, manifest, tofu, graph, err := a.Admission.Database.GetDeployment(ctx, tx, claims.OrgID, head.Id, model.GetModeDefault)
	if err != nil {
		return writeObservationError(c, err)
	}
	if base.Id != body.BaselineId || base.DeploymentEnvUuid != body.EnvironmentUuid || base.Status != model.DeploymentStatusSucceeded || !base.CompletedAt.IsSet() || (base.Mode != model.DeploymentModeDeploy && base.Mode != model.DeploymentModeRollback) ||
		!model.CompleteDeploymentSnapshot(manifest, tofu, graph) ||
		model.ObservationBaselineFingerprint(manifest, tofu, graph, base.ModuleArtifacts) != body.BaselineFingerprint {
		return writeObservationError(c, model.NewErrConflict("latest successful baseline no longer matches finding"))
	}
	env, err := a.ControlPlaneClient.GetEnvironmentWithResponse(ctx, claims.OrgID, body.ProjectId, body.EnvId)
	if err != nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "Core Environment lookup unavailable")
	}
	if env.StatusCode() != http.StatusOK || env.JSON200 == nil || env.JSON200.ProjectUuid == nil || *env.JSON200.ProjectUuid != body.ProjectUuid || env.JSON200.Uuid != body.EnvironmentUuid || env.JSON200.RunnerId == nil || string(env.JSON200.Status) == "deleting" {
		return writeObservationError(c, model.NewErrConflict("current Environment identity or lifecycle changed"))
	}
	for _, artifact := range base.ModuleArtifacts {
		if !artifact.HasObservationProvenance() {
			return writeObservationError(c, model.NewErrConflict("baseline has unavailable immutable module material"))
		}
	}
	attempt := a.OrdinaryAdmissions.NewAttempt(claims.OrgID, body.ProjectUuid, body.EnvironmentUuid, actor, model.DeploymentModeDeploy, "observation-redeploy:"+claims.PluginID+":"+body.RequestId.String())
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
			_ = attempt.SealAbsent(context.WithoutCancel(ctx))
		}
	}()
	carry := make(map[string]uuid.UUID, len(base.ModuleArtifacts))
	for _, artifact := range base.ModuleArtifacts {
		carry[artifact.ModuleUUID.String()] = artifact.VersionUUID
	}
	if err := attempt.Authorize(
		ctx,
		tx,
		base.ModuleArtifacts,
		ordinaryDeploymentCompiledIntent{ProjectID: body.ProjectId, EnvironmentID: body.EnvId, RunnerID: *env.JSON200.RunnerId, EffectiveBaseDeploymentID: base.Id, Manifest: manifest, Graph: graph, Tofu: tofu, RunnerLogLevel: "info", CarryForwardModuleVersions: carry},
	); err != nil {
		return writeObservationError(c, err)
	}
	dep, err := a.Admission.Database.CreateDeployment(
		ctx,
		tx,
		claims.OrgID,
		body.ProjectId,
		body.EnvId,
		model.CreateDeploymentParams{
			CreatedBy:                  actor,
			DeploymentEnvUuid:          body.EnvironmentUuid,
			Mode:                       model.DeploymentModeDeploy,
			Manifest:                   manifest,
			Graph:                      graph,
			Tofu:                       tofu,
			RunnerId:                   *env.JSON200.RunnerId,
			RunnerLogLevel:             "info",
			ModuleArtifacts:            base.ModuleArtifacts,
			ModuleVersionRequestDigest: base.ModuleVersionRequestDigest,
			Metrics:                    base.Metrics,
			ExpectedBaseDeploymentID:   opt.Of(base.Id),
		},
	)
	if err != nil {
		return writeObservationError(c, err)
	}
	if err := attempt.Bind(ctx, tx, dep.Id); err != nil {
		return err
	}
	parsed, err := graphs.FromJson(bytes.NewReader(graph))
	if err != nil {
		return err
	}
	if err := a.Admission.Database.InitActiveResourcesFromGraph(ctx, tx, body.EnvironmentUuid, dep.Id, parsed); err != nil {
		return err
	}
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO environment_observation_redeploy_receipts(org_id,plugin_id,request_id,run_id,actor_id,request_sha256,deployment_id,baseline_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
		claims.OrgID,
		claims.PluginID,
		body.RequestId,
		runID,
		actor,
		digest,
		dep.Id,
		base.Id,
	); err != nil {
		return err
	}
	pending, err := a.Admission.Database.InsertPendingEventMessages(ctx, tx, []*hstandardoutbox.PendingEventMessage{{Subject: string(genevents.IoPlatformOrchestratorDeploymentCreated), Payload: model.ConvertDeploymentToEventPayload(dep)}})
	if err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	reliableoutbox.OptimisticPublish(ctx, a.logger(), a.Admission.Database.AsReliableOutboxStore(), a.EventPublisher, pending)
	return c.JSON(http.StatusOK, observation.ObservationRedeployReceipt{RequestId: body.RequestId, DeploymentId: dep.Id, BaselineId: base.Id, Status: observation.ObservationRedeployReceiptStatus(dep.Status)})
}

const ObservationRedeployReadPath = "/orgs/:orgId/plugins/:pluginId/observations/v1/redeployments/:requestId"
const ObservationRedeployReadAction = "observation.redeploy.get"

func (a *ExternalDeploymentHTTPAdapter) HandleObservationRedeployRead(c echo.Context) error {
	claims, err := a.authenticateObservation(c, ObservationRedeployReadAction, nil, "")
	if err != nil {
		return err
	}
	requestID, err := uuid.Parse(c.Param("requestId"))
	if err != nil || requestID == uuid.Nil {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid redeploy request")
	}
	ctx, cancel := context.WithTimeout(c.Request().Context(), RequestHandlerTimeout)
	defer cancel()
	tx, err := a.Admission.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var out observation.ObservationRedeployReceipt
	err = tx.QueryRowContext(ctx, `SELECT r.request_id,r.deployment_id,r.baseline_id,COALESCE(d.status,'unavailable') FROM environment_observation_redeploy_receipts r LEFT JOIN deployments d ON d.id=r.deployment_id WHERE r.org_id=$1 AND r.plugin_id=$2 AND r.request_id=$3`, claims.OrgID, claims.PluginID, requestID).
		Scan(&out.RequestId, &out.DeploymentId, &out.BaselineId, &out.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return writeObservationError(c, model.NewErrNotFound("redeploy request not found"))
	}
	if err != nil {
		return err
	}
	return c.JSON(http.StatusOK, out)
}
