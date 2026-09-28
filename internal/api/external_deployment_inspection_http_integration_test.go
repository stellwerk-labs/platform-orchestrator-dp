//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestExternalReadinessRequiresExactSignedBodyAndReturnsImmutableInventory(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := externalDeploymentTestDatabase(t, ctx, connection)

	const orgID, pluginID, projectID, envID = "acme", "dev.example.rollouts", "checkout", "production"
	projectUUID, environmentUUID, moduleUUID, versionUUID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	deployment, err := store.CreateDeployment(ctx, tx, orgID, projectID, envID, model.CreateDeploymentParams{
		CreatedBy: uuid.New(), DeploymentEnvUuid: environmentUUID, Mode: model.DeploymentModeDeploy,
		Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`), Graph: model.EncodedDeploymentGraph(`{"nodes":{},"edges":{},"workloads":{},"shared_resources":{},"leaf_resources":{}}`),
		Tofu: model.RawTofu("terraform {}"), RunnerId: "runner", RunnerLogLevel: "info", IdempotencyKeyDigest: opt.Empty[string](),
		ModuleArtifacts: []model.ModuleArtifactRequirement{{ModuleID: "postgres", ModuleUUID: moduleUUID, Version: "v2", VersionUUID: versionUUID, SemanticVersion: "2.0.0", ArtifactDigest: "sha256:abc", Source: "private-source"}},
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, deployment.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, StatusMessage: "done"})
	require.NoError(t, err)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	coreServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/orgs/acme/projects/checkout/envs/production" {
			http.NotFound(response, request)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"created_at": time.Now(), "updated_at": time.Now(), "display_name": "Production", "env_type_id": "production",
			"id": envID, "project_id": projectID, "project_uuid": projectUUID, "status": "active", "uuid": environmentUUID,
		})
	}))
	t.Cleanup(coreServer.Close)
	cpClient, err := platformorchestratorcp.NewClientWithResponses(coreServer.URL)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	adapter := &ExternalDeploymentHTTPAdapter{
		Issuer: "platform-orchestrator-cp", VerificationKey: publicKey,
		Admission:          ExternalDeploymentAdmissionService{Database: store, Authorizer: CoreExternalDeploymentGrantClient{}, Logger: zap.NewNop()},
		ControlPlaneClient: cpClient, EventPublisher: &hmessaging.RecordingPublisher{}, Logger: zap.NewNop(), Now: func() time.Time { return now },
	}
	server := echo.New()
	server.POST(ExternalDeploymentReadinessPath, adapter.HandleReadiness)
	server.GET(ExternalDeploymentReadPath, adapter.HandleRead)
	body, err := json.Marshal(externalDeploymentReadinessBody{Environments: []externalEnvironmentRef{{
		ProjectID: projectID, EnvID: envID, ProjectUUID: projectUUID, EnvironmentUUID: environmentUUID,
	}}})
	require.NoError(t, err)
	path, requestID := "/orgs/acme/plugins/dev.example.rollouts/external-deployment-readiness", "readiness-1"
	claims := plugincontext.Claims{
		Version: 1, Issuer: "platform-orchestrator-cp", Audience: ExternalDeploymentAudience,
		PluginID: pluginID, OrgID: orgID, ActorID: uuid.NewString(), OperationID: ExternalDeploymentReadinessAction,
		Method: http.MethodPost, Path: path, BodySHA256: plugincontext.Digest(body), RequestID: requestID,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(plugincontext.MaxLifetime).Unix(),
	}
	assertion, err := plugincontext.Sign(privateKey, claims, now)
	require.NoError(t, err)
	do := func(payload []byte) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", requestID)
		request.Header.Set(plugincontext.Header, assertion)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		return response
	}

	response := do(body)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), "private-source")
	var decoded externalDeploymentReadinessResponse
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &decoded))
	require.Equal(t, "ready", decoded.Items[0].State)
	require.Equal(t, moduleUUID, *decoded.Items[0].ActiveModules[0].ModuleUUID)
	require.Equal(t, versionUUID, *decoded.Items[0].ActiveModules[0].VersionUUID)

	tampered := bytes.Replace(body, []byte(envID), []byte("staging"), 1)
	require.Equal(t, http.StatusUnauthorized, do(tampered).Code)

	readPath := "/orgs/acme/plugins/dev.example.rollouts/projects/" + projectUUID.String() + "/envs/" + environmentUUID.String() + "/external-deployments/" + deployment.Id.String()
	readClaims := claims
	readClaims.OperationID = ExternalDeploymentReadAction
	readClaims.Method = http.MethodGet
	readClaims.Path = readPath
	readClaims.BodySHA256 = plugincontext.Digest(nil)
	readClaims.RequestID = "read-1"
	readAssertion, err := plugincontext.Sign(privateKey, readClaims, now)
	require.NoError(t, err)
	readRequest := httptest.NewRequest(http.MethodGet, readPath, nil)
	readRequest.Header.Set(plugincontext.Header, readAssertion)
	readResponse := httptest.NewRecorder()
	server.ServeHTTP(readResponse, readRequest)
	require.Equal(t, http.StatusOK, readResponse.Code, readResponse.Body.String())
	require.Contains(t, readResponse.Body.String(), deployment.Id.String())
	require.NotContains(t, readResponse.Body.String(), "private-source")
}
