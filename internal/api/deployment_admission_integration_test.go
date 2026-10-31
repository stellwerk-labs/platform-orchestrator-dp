//go:build integration

package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestOrdinaryDeploymentAdmissionUsesExactCoreGuardAndRecoversUnknownOutcome(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := deploymentAdmissionTestDatabase(t, ctx, connection)

	const orgID = "admission-org"
	projectUUID, environmentUUID, actorID := uuid.New(), uuid.New(), uuid.New()
	var lock sync.Mutex
	authorizeBodies := make(map[uuid.UUID]platformorchestratorcp.DeploymentAdmissionRequest)
	activeAdmission := uuid.Nil
	deniedAdmission := uuid.Nil
	deniedStatusCode := http.StatusConflict
	core := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !assert.Equal(t, "Bearer scoped-dp-token", request.Header.Get("Authorization")) {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
		if !assert.Contains(t, []int{4, 5}, len(parts)) ||
			!assert.Equal(t, []string{"orgs", orgID, "deployment-admissions"}, parts[:3]) {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		admissionID, err := uuid.Parse(parts[3])
		if !assert.NoError(t, err) {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		if len(parts) == 5 {
			if parts[4] == "settle" {
				var settlement platformorchestratorcp.SettleDeploymentAdmissionJSONRequestBody
				if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&settlement)) {
					writer.WriteHeader(http.StatusBadRequest)
					return
				}
				lock.Lock()
				original := authorizeBodies[admissionID]
				lock.Unlock()
				validProject := assert.Equal(t, original.ProjectUuid, settlement.ProjectUuid)
				validEnvironment := assert.Equal(t, original.EnvironmentUuid, settlement.EnvironmentUuid)
				validDigest := assert.Equal(t, original.RequestDigest, settlement.RequestDigest)
				if !validProject || !validEnvironment || !validDigest {
					writer.WriteHeader(http.StatusConflict)
					return
				}
				writer.WriteHeader(http.StatusOK)
				assert.NoError(t, json.NewEncoder(writer).Encode(platformorchestratorcp.DeploymentAdmissionReceipt{
					AdmissionId: admissionID, ProjectUuid: settlement.ProjectUuid, EnvironmentUuid: settlement.EnvironmentUuid,
					RequestDigest: settlement.RequestDigest, Mode: string(original.Mode), Epoch: 21,
					State: platformorchestratorcp.AdmissionStateSettled, Outcome: refString(string(settlement.Outcome)),
				}))
				return
			}
			if !assert.Equal(t, "seal-absence", parts[4]) {
				writer.WriteHeader(http.StatusNotFound)
				return
			}
			var body platformorchestratorcp.DeploymentAdmissionRequest
			if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&body)) {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			writer.WriteHeader(http.StatusOK)
			assert.NoError(t, json.NewEncoder(writer).Encode(platformorchestratorcp.DeploymentAdmissionReceipt{
				AdmissionId: admissionID, ProjectUuid: body.ProjectUuid, EnvironmentUuid: body.EnvironmentUuid,
				RequestDigest: body.RequestDigest, Mode: string(body.Mode), Epoch: 22,
				State: platformorchestratorcp.AdmissionStateSettled, Outcome: refString(string(platformorchestratorcp.AdmissionOutcomeAbsent)),
			}))
			return
		}
		var body platformorchestratorcp.DeploymentAdmissionRequest
		if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&body)) {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		lock.Lock()
		authorizeBodies[admissionID] = body
		lock.Unlock()
		if admissionID == deniedAdmission {
			writer.WriteHeader(deniedStatusCode)
			_, _ = writer.Write([]byte(`{"message":"Deployment admission denied"}`))
			return
		}
		if admissionID != activeAdmission {
			writer.WriteHeader(http.StatusServiceUnavailable)
			_, _ = writer.Write([]byte(`{"message":"unknown outcome"}`))
			return
		}
		writer.WriteHeader(http.StatusOK)
		assert.NoError(t, json.NewEncoder(writer).Encode(platformorchestratorcp.DeploymentAdmissionReceipt{
			AdmissionId: admissionID, ProjectUuid: body.ProjectUuid, EnvironmentUuid: body.EnvironmentUuid,
			RequestDigest: body.RequestDigest, Mode: string(body.Mode), Epoch: 21,
			State: platformorchestratorcp.AdmissionStateActive,
		}))
	}))
	defer core.Close()
	tokenFile := t.TempDir() + "/token"
	require.NoError(t, os.WriteFile(tokenFile, []byte("scoped-dp-token\n"), 0o600))
	service, err := NewOrdinaryDeploymentAdmissionService(store, core.URL, tokenFile, core.Client())
	require.NoError(t, err)

	artifacts := []model.ModuleArtifactRequirement{{ModuleID: "postgres", ModuleUUID: uuid.New(), Version: "v1", VersionUUID: uuid.New(), ArtifactDigest: "sha256:optional"}}
	carryForwardVersions := map[string]uuid.UUID{artifacts[0].ModuleUUID.String(): artifacts[0].VersionUUID}
	confirmedRestrictedVersions := []uuid.UUID{artifacts[0].VersionUUID}
	compiled := ordinaryDeploymentCompiledIntent{
		ProjectID: "project", EnvironmentID: "environment", RunnerID: "runner", Manifest: []byte(`{"workloads":{"api":{"params":{"secret":"not-sent"}}}}`),
		Graph: []byte(`{"nodes":{"api":{}}}`), Tofu: []byte("terraform { required_version = \">= 1.9\" }"), RunnerLogLevel: "info",
		CarryForwardModuleVersions: carryForwardVersions, ConfirmedRestrictedVersionUUIDs: confirmedRestrictedVersions,
	}
	attempt := service.NewAttempt(orgID, projectUUID, environmentUUID, actorID, model.DeploymentModeDeploy, "stable-command")
	activeAdmission = attempt.AdmissionID
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, attempt.Authorize(ctx, tx, artifacts, compiled))
	deployment, err := store.CreateDeployment(ctx, tx, orgID, compiled.ProjectID, compiled.EnvironmentID, model.CreateDeploymentParams{
		CreatedBy: actorID, DeploymentEnvUuid: environmentUUID, Mode: model.DeploymentModeDeploy,
		Manifest: model.EncodedDeploymentManifest(compiled.Manifest), Graph: model.EncodedDeploymentGraph(compiled.Graph), Tofu: compiled.Tofu,
		RunnerId: compiled.RunnerID, RunnerLogLevel: compiled.RunnerLogLevel, IdempotencyKeyDigest: opt.Empty[string](), ModuleArtifacts: artifacts,
	})
	require.NoError(t, err)
	require.NoError(t, attempt.Bind(ctx, tx, deployment.Id))
	require.NoError(t, tx.Commit())
	lock.Lock()
	activeBody := authorizeBodies[activeAdmission]
	lock.Unlock()
	require.Equal(t, actorID, activeBody.ActorId)
	require.Equal(t, artifacts[0].VersionUUID, activeBody.ModuleVersions[artifacts[0].ModuleUUID.String()])
	require.Equal(t, carryForwardVersions, *activeBody.CarryForwardModuleVersions)
	require.Equal(t, confirmedRestrictedVersions, *activeBody.ConfirmedRestrictedVersionUuids)
	require.NotContains(t, activeBody.RequestDigest, "not-sent")
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, deployment.Id, model.UpdateDeploymentStatusAndOutputsParams{
		Status: model.DeploymentStatusSucceeded, StatusMessage: "done", Outputs: opt.Empty[string](), Metrics: model.DeploymentMetrics{},
	})
	require.NoError(t, err)
	backdateAdmissionReceipt(t, ctx, store, orgID, activeAdmission)
	require.NoError(t, service.ReconcileUnsettled(ctx, 100))
	settledTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	settled, err := model.GetDeploymentAdmissionReceipt(ctx, settledTx, orgID, activeAdmission)
	require.NoError(t, err)
	require.Equal(t, "succeeded", settled.TerminalOutcome)
	require.Equal(t, int64(21), settled.CoreEpoch)
	require.True(t, settled.CoreSettledAt.Valid)
	require.NoError(t, settledTx.Commit())

	changed := service.NewAttempt(orgID, projectUUID, environmentUUID, actorID, model.DeploymentModeDeploy, "stable-command")
	require.Equal(t, attempt.AdmissionID, changed.AdmissionID)
	changedTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	changedCompiled := compiled
	changedCompiled.Graph = []byte(`{"nodes":{"changed":{}}}`)
	err = changed.Authorize(ctx, changedTx, artifacts, changedCompiled)
	_, conflict := model.IsErrConflict(err)
	require.True(t, conflict, "same key with changed compiled intent must conflict: %v", err)
	require.NoError(t, changedTx.Rollback())
	require.NoError(t, changed.SealAbsent(ctx), "a rejected changed intent must not seal the original receipt")
	unchangedTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	unchanged, err := model.GetDeploymentAdmissionReceipt(ctx, unchangedTx, orgID, activeAdmission)
	require.NoError(t, err)
	require.Equal(t, "committed", unchanged.Disposition)
	require.Equal(t, deployment.Id, unchanged.DeploymentID.UUID)
	require.NoError(t, unchangedTx.Commit())

	unknown := service.NewAttempt(orgID, projectUUID, environmentUUID, actorID, model.DeploymentModeRollback, "")
	unknownTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	err = unknown.Authorize(ctx, unknownTx, artifacts, compiled)
	require.ErrorIs(t, err, ErrDeploymentAdmissionUnavailable)
	require.NoError(t, unknownTx.Rollback())
	require.NoError(t, unknown.SealAbsent(ctx))
	assertSealedAdmission(t, ctx, store, orgID, unknown.AdmissionID, 22)

	denied := service.NewAttempt(orgID, projectUUID, environmentUUID, actorID, model.DeploymentModeDeploy, "denied-by-core")
	deniedAdmission = denied.AdmissionID
	deniedTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	err = denied.Authorize(ctx, deniedTx, artifacts, compiled)
	require.ErrorIs(t, err, ErrDeploymentAdmissionDenied)
	require.NoError(t, deniedTx.Rollback())
	require.NoError(t, denied.SealAbsent(ctx))
	assertSealedAdmission(t, ctx, store, orgID, denied.AdmissionID, 22)

	permissionLost := service.NewAttempt(orgID, projectUUID, environmentUUID, actorID, model.DeploymentModeDeploy, "permission-lost")
	deniedAdmission = permissionLost.AdmissionID
	deniedStatusCode = http.StatusForbidden
	permissionTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	err = permissionLost.Authorize(ctx, permissionTx, artifacts, compiled)
	var deniedError *deploymentAdmissionDeniedError
	require.ErrorAs(t, err, &deniedError)
	require.Equal(t, http.StatusForbidden, deniedError.statusCode)
	require.NoError(t, permissionTx.Rollback())
	require.NoError(t, permissionLost.SealAbsent(ctx))
	assertSealedAdmission(t, ctx, store, orgID, permissionLost.AdmissionID, 22)

	crashed := service.NewAttempt(orgID, projectUUID, environmentUUID, actorID, model.DeploymentModeDeploy, "")
	crashedTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	err = crashed.Authorize(ctx, crashedTx, artifacts, compiled)
	require.ErrorIs(t, err, ErrDeploymentAdmissionUnavailable)
	require.NoError(t, crashedTx.Rollback())
	backdateAdmissionReceipt(t, ctx, store, orgID, crashed.AdmissionID)
	restarted, err := NewOrdinaryDeploymentAdmissionService(store, core.URL, tokenFile, core.Client())
	require.NoError(t, err)
	require.NoError(t, restarted.ReconcileUnsettled(ctx, 100))
	assertSealedAdmission(t, ctx, store, orgID, crashed.AdmissionID, 22)
}

func deploymentAdmissionTestDatabase(t *testing.T, ctx context.Context, connection string) model.Databaser {
	schema := "admission_api_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	return createAdmissionSchemaDatabase(t, ctx, connection, schema)
}

func createAdmissionSchemaDatabase(t *testing.T, ctx context.Context, connection, schema string) model.Databaser {
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	store, err := model.NewDatabaser(ctx, zap.NewNop(), connection+" search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func assertSealedAdmission(t *testing.T, ctx context.Context, store model.Databaser, orgID string, admissionID uuid.UUID, epoch int64) {
	t.Helper()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	receipt, err := model.GetDeploymentAdmissionReceipt(ctx, tx, orgID, admissionID)
	require.NoError(t, err)
	require.Equal(t, "absent", receipt.Disposition)
	require.Equal(t, "absent", receipt.TerminalOutcome)
	require.Equal(t, int64(epoch), receipt.CoreEpoch)
	require.True(t, receipt.CoreSettledAt.Valid)
	require.NoError(t, tx.Commit())
}

func backdateAdmissionReceipt(t *testing.T, ctx context.Context, store model.Databaser, orgID string, admissionID uuid.UUID) {
	t.Helper()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployment_admission_receipts SET created_at=now()-interval '2 minutes' WHERE org_id=$1 AND admission_id=$2`, orgID, admissionID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func refString(value string) *string { return &value }
