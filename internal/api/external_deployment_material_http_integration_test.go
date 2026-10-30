//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
)

func TestExternalDeploymentMaterialRequiresExactReceiptAndPreservesHistoricalAccess(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := externalDeploymentTestDatabase(t, ctx, connection)

	const orgID, pluginID, projectID, envID = "acme", "dev.example.rollouts", "checkout", "deleted-production"
	projectUUID, environmentUUID, operationID := uuid.New(), uuid.New(), uuid.New()
	actorID := uuid.New()
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	encrypt := func(plaintext string) string {
		var encrypted bytes.Buffer
		writer, encryptErr := age.Encrypt(&encrypted, identity.Recipient())
		require.NoError(t, encryptErr)
		_, encryptErr = writer.Write([]byte(plaintext))
		require.NoError(t, encryptErr)
		require.NoError(t, writer.Close())
		return base64.StdEncoding.EncodeToString(encrypted.Bytes())
	}
	logs, outputs := "retained runner logs\n", `{"endpoint":"https://private.invalid","password":"secret"}`
	encodedLogs, encodedOutputs := encrypt(logs), encrypt(outputs)

	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	deployment, err := store.CreateDeployment(ctx, tx, orgID, projectID, envID, model.CreateDeploymentParams{
		CreatedBy: actorID, DeploymentEnvUuid: environmentUUID, Mode: model.DeploymentModeDeploy,
		Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`),
		Graph:    model.EncodedDeploymentGraph(`{"nodes":{},"edges":{},"workloads":{},"shared_resources":{},"leaf_resources":{}}`),
		Tofu:     model.RawTofu("terraform {}"), IdempotencyKeyDigest: opt.Empty[string](), RunnerId: "runner", RunnerLogLevel: "info",
		EncryptedOutputsRecipient: opt.Of(identity.Recipient().String()), EncryptedLogsRecipient: opt.Of(identity.Recipient().String()),
	})
	require.NoError(t, err)
	_, err = model.StoreExternalOperationMaterial(ctx, tx, model.ExternalOperationMaterial{
		OrgID: orgID, PluginID: pluginID, OperationRef: operationID, Recipient: identity.Recipient().String(), EncryptedIdentity: "vault:v1:wrapped",
	})
	require.NoError(t, err)
	_, err = model.CreateExternalOperationDeploymentReceipt(ctx, tx, model.ExternalOperationDeploymentReceipt{
		ExternalOperationDeploymentKey: model.ExternalOperationDeploymentKey{OrgID: orgID, PluginID: pluginID, RequestID: "dispatch-material"},
		OperationRef:                   operationID.String(), Fence: 7, AssertionAction: ExternalDeploymentAction, ServiceAction: ExternalDeploymentAction,
		RequestBodySHA256: strings.Repeat("a", 64), ActorID: actorID.String(), ProjectID: projectID, EnvID: envID,
		ProjectUUID: opt.Of(projectUUID), EnvironmentUUID: opt.Of(environmentUUID), BaseDeploymentID: opt.Of(deployment.Id), DeploymentID: deployment.Id,
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	resultServer := Server{Database: store, Logger: zap.NewNop(), EventPublisher: &hmessaging.RecordingPublisher{}}
	require.NoError(t, resultServer.ApplyRunnerDeploymentResult(ctx, orgID, "runner", deployment.Id, uuid.NewString(), DeploymentResultsUpdateBody{
		Status: Success, Outputs: &encodedOutputs,
	}))
	// Exercise the real transient cleaner after the normal five-minute window,
	// without sleeping. The existing ownership must preserve the exact ciphertext.
	retentionTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = retentionTx.ExecContext(ctx, "UPDATE deployments SET completed_at=now()-interval '10 minutes' WHERE id=$1", deployment.Id)
	require.NoError(t, err)
	require.NoError(t, retentionTx.Commit())
	cleaned, err := store.DeleteDeploymentOutputsByCompletionDate(ctx, nil, time.Now().Add(-5*time.Minute))
	require.NoError(t, err)
	require.Empty(t, cleaned, "historical external material is not transient user-key output")
	retained, err := store.GetDeploymentEncryptedOutputs(ctx, nil, orgID, deployment.Id)
	require.NoError(t, err)
	require.Equal(t, encodedOutputs, retained, "retain original Runner ciphertext, never reconstructed outputs")

	vaultServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		validMethod := assert.Equal(t, http.MethodPut, request.Method)
		validPath := assert.Equal(t, "/v1/transit/decrypt/"+externalOperationMaterialTransitKey, request.URL.Path)
		if !validMethod || !validPath {
			response.WriteHeader(http.StatusNotFound)
			return
		}
		var body map[string]string
		if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&body)) ||
			!assert.Equal(t, "vault:v1:wrapped", body["ciphertext"]) {
			response.WriteHeader(http.StatusBadRequest)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(response).Encode(map[string]any{"data": map[string]string{
			"plaintext": base64.StdEncoding.EncodeToString([]byte(identity.String())),
		}}))
	}))
	t.Cleanup(vaultServer.Close)
	vaultConfig := vaultapi.DefaultConfig()
	vaultConfig.Address, vaultConfig.HttpClient = vaultServer.URL, vaultServer.Client()
	vaultClient, err := vaultapi.NewClient(vaultConfig)
	require.NoError(t, err)
	vaultClient.SetToken("test-token")

	var liveEnvironmentLookups atomic.Int64
	coreServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		liveEnvironmentLookups.Add(1)
		response.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(coreServer.Close)
	cpClient, err := platformorchestratorcp.NewClientWithResponses(coreServer.URL)
	require.NoError(t, err)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Unix(2_000_000_000, 0)
	adapter := &ExternalDeploymentHTTPAdapter{
		Issuer: "platform-orchestrator-cp", VerificationKey: publicKey,
		Admission:          ExternalDeploymentAdmissionService{Database: store, Authorizer: CoreExternalDeploymentGrantClient{}, Logger: zap.NewNop()},
		Materials:          ExternalOperationMaterialService{Vault: vault.NewVaultClient(vaultClient, zap.NewNop())},
		ControlPlaneClient: cpClient, EventPublisher: &hmessaging.RecordingPublisher{}, Logger: zap.NewNop(), Now: func() time.Time { return now },
		RunnerLogsReader: func(_ context.Context, filename string) (io.ReadCloser, error) {
			require.Equal(t, environmentUUID.String()+"/"+deployment.Id.String(), filename)
			return io.NopCloser(strings.NewReader(encodedLogs)), nil
		},
	}
	server := echo.New()
	server.GET(ExternalDeploymentLogsPath, adapter.HandleLogs)
	server.GET(ExternalDeploymentOutputsPath, adapter.HandleOutputs)
	basePath := "/orgs/" + orgID + "/plugins/" + pluginID + "/projects/" + projectUUID.String() + "/envs/" + environmentUUID.String() + "/external-deployments/" + deployment.Id.String()
	do := func(path, action string) *httptest.ResponseRecorder {
		claims := plugincontext.Claims{
			Version: 1, Issuer: "platform-orchestrator-cp", Audience: ExternalDeploymentAudience,
			PluginID: pluginID, OrgID: orgID, ActorID: actorID.String(), OperationID: action,
			Method: http.MethodGet, Path: path, BodySHA256: plugincontext.Digest(nil), RequestID: uuid.NewString(),
			IssuedAt: now.Unix(), ExpiresAt: now.Add(plugincontext.MaxLifetime).Unix(),
		}
		assertion, signErr := plugincontext.Sign(privateKey, claims, now)
		require.NoError(t, signErr)
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set(plugincontext.Header, assertion)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		return response
	}

	logsResponse := do(basePath+"/logs", ExternalDeploymentLogsAction)
	require.Equal(t, http.StatusOK, logsResponse.Code, logsResponse.Body.String())
	require.Equal(t, logs, logsResponse.Body.String())
	require.Equal(t, "no-store", logsResponse.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", logsResponse.Header().Get("X-Content-Type-Options"))
	require.Contains(t, logsResponse.Header().Get("Content-Disposition"), deployment.Id.String()+".log")
	outputsResponse := do(basePath+"/outputs", ExternalDeploymentOutputsAction)
	require.Equal(t, http.StatusOK, outputsResponse.Code, outputsResponse.Body.String())
	require.Equal(t, outputs, outputsResponse.Body.String())
	require.Contains(t, outputsResponse.Header().Get("Content-Disposition"), deployment.Id.String()+".outputs.json")
	require.Zero(t, liveEnvironmentLookups.Load(), "historical evidence authorization is Core's responsibility and must not depend on a live slug lookup in DP")

	wrongEnvironmentPath := strings.Replace(basePath, environmentUUID.String(), uuid.NewString(), 1) + "/logs"
	require.Equal(t, http.StatusNotFound, do(wrongEnvironmentPath, ExternalDeploymentLogsAction).Code)
	require.Equal(t, http.StatusUnauthorized, do(basePath+"/outputs", ExternalDeploymentLogsAction).Code, "one material action must not authorize the other")
}
