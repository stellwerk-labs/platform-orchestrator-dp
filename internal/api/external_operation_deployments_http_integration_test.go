//go:build integration

package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hmessaging"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratorcpv2 "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/plugincontext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestExternalDeploymentHTTPAdapterCreatesNormalDeploymentAndReauthorizesReplay(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := externalDeploymentTestDatabase(t, ctx, connection)

	now := time.Unix(2_000_000_000, 0)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	operationID := uuid.New()
	actorID := uuid.New()
	projectUUID := uuid.New()
	environmentUUID := uuid.New()
	const (
		orgID      = "acme"
		pluginID   = "dev.example.rollouts"
		projectID  = "checkout"
		envID      = "production"
		dispatchID = "dispatch-1"
		runnerID   = "runner-1"
	)
	var checks atomic.Int64
	var grantActive atomic.Bool
	grantActive.Store(true)

	coreServer := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/orgs/acme/projects/checkout/envs/production":
			_ = json.NewEncoder(response).Encode(map[string]any{
				"created_at": now, "updated_at": now, "display_name": "Production", "env_type_id": "production",
				"id": envID, "project_id": projectID, "project_uuid": projectUUID, "runner_id": runnerID,
				"status": "active", "uuid": environmentUUID,
			})
		case request.Method == http.MethodPost && request.URL.Path == "/internal/orgs/acme/projects/checkout/envs/production/module-catalogue":
			_, _ = response.Write([]byte(`{"modules":[],"providers":[]}`))
		case request.Method == http.MethodGet && request.URL.Path == "/orgs/acme/runners/runner-1":
			_ = json.NewEncoder(response).Encode(map[string]any{
				"created_at": now, "updated_at": now, "id": runnerID, "org_id": orgID,
				"runner_configuration": map[string]any{
					"type": "kubernetes", "cluster": map[string]any{"auth": map[string]any{}, "cluster_data": map[string]any{"certificate_authority_data": "", "server": ""}},
					"job": map[string]any{"namespace": "runner", "service_account": "runner"},
				},
				"state_storage_configuration": map[string]any{"type": "kubernetes", "namespace": "state"},
			})
		case request.Method == http.MethodPost && request.URL.Path == "/orgs/acme/plugins/dev.example.rollouts/operations/"+operationID.String()+"/check":
			checks.Add(1)
			if !assert.Equal(t, "Bearer dp-service-token", request.Header.Get("Authorization")) {
				response.WriteHeader(http.StatusUnauthorized)
				return
			}
			if !grantActive.Load() {
				response.WriteHeader(http.StatusForbidden)
				return
			}
			var check platformorchestratorcpv2.PluginDispatchCheck
			if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&check)) {
				response.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(response).Encode(platformorchestratorcpv2.PluginDispatchAuthorization{
				Action: string(check.Action), ActorId: actorID, DispatchId: check.DispatchId,
				EnvironmentUuid: check.EnvironmentUuid, Fence: check.Fence, OperationId: operationID,
				OrgId: orgID, PluginId: pluginID, ProjectUuid: check.ProjectUuid, RequestDigest: check.RequestDigest,
			})
		default:
			http.Error(response, "unexpected Core test request", http.StatusNotFound)
		}
	}))
	t.Cleanup(coreServer.Close)
	cpClient, err := platformorchestratorcp.NewClientWithResponses(coreServer.URL)
	require.NoError(t, err)
	coreURL, err := url.Parse(coreServer.URL)
	require.NoError(t, err)
	grantClient := CoreExternalDeploymentGrantClient{BaseURL: coreURL, HTTPClient: coreServer.Client(), BearerToken: "dp-service-token"}
	publisher := &hmessaging.RecordingPublisher{}
	adapter := &ExternalDeploymentHTTPAdapter{
		Issuer: "platform-orchestrator-cp", VerificationKey: publicKey,
		Admission:          ExternalDeploymentAdmissionService{Database: store, Authorizer: grantClient, Logger: zap.NewNop()},
		ControlPlaneClient: cpClient, EventPublisher: publisher, Logger: zap.NewNop(), Now: func() time.Time { return now },
	}
	server := echo.New()
	server.POST(ExternalDeploymentAdmissionPath, adapter.Handle)

	bodyBytes, err := json.Marshal(ExternalDeploymentAdmissionBody{
		OperationRef: operationID.String(), Fence: 7,
		Deployment: DeploymentCreateBody{
			Mode: DeploymentCreateBodyModeDeploy, ProjectId: projectID, EnvId: envID,
			Manifest: &DeploymentManifest{Workloads: map[string]DeploymentManifestWorkload{}},
		},
	})
	require.NoError(t, err)
	path := "/orgs/acme/plugins/dev.example.rollouts/external-deployments"
	claims := plugincontext.Claims{
		Version: 1, Issuer: "platform-orchestrator-cp", Audience: ExternalDeploymentAudience,
		PluginID: pluginID, OrgID: orgID, ActorID: actorID.String(), OperationID: ExternalDeploymentAction,
		Method: http.MethodPost, Path: path, BodySHA256: plugincontext.Digest(bodyBytes), RequestID: dispatchID,
		IssuedAt: now.Unix(), ExpiresAt: now.Add(plugincontext.MaxLifetime).Unix(),
	}
	assertion, err := plugincontext.Sign(privateKey, claims, now)
	require.NoError(t, err)
	do := func(body []byte, signed string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", dispatchID)
		request.Header.Set(plugincontext.Header, signed)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		return response
	}

	created := do(bodyBytes, assertion)
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	var createdBody ExternalDeploymentAdmissionResponse
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &createdBody))
	require.False(t, createdBody.Replayed)
	deployment, _, _, _, err := store.GetDeployment(ctx, nil, orgID, createdBody.DeploymentID, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, actorID, deployment.CreatedBy)
	require.Equal(t, model.DeploymentModeDeploy, deployment.Mode)

	replayed := do(bodyBytes, assertion)
	require.Equal(t, http.StatusOK, replayed.Code, replayed.Body.String())
	require.EqualValues(t, 2, checks.Load(), "replay must reauthorize before loading its receipt")

	grantActive.Store(false)
	revoked := do(bodyBytes, assertion)
	require.Equal(t, http.StatusForbidden, revoked.Code, revoked.Body.String())
	require.EqualValues(t, 3, checks.Load())

	tampered := append([]byte(nil), bodyBytes...)
	tampered = bytes.Replace(tampered, []byte(`"fence":7`), []byte(`"fence":8`), 1)
	invalid := do(tampered, assertion)
	require.Equal(t, http.StatusUnauthorized, invalid.Code, invalid.Body.String())
	require.EqualValues(t, 3, checks.Load(), "invalid assertion must fail before Core callbacks")
}

func externalDeploymentTestDatabase(t *testing.T, ctx context.Context, connection string) model.Databaser {
	t.Helper()
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "external_http_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	store, err := model.NewDatabaser(ctx, zap.NewNop(), fmt.Sprintf("%s search_path=%s", connection, schema))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}
