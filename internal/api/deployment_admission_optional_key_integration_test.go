//go:build integration

package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hmessaging"
	coreapi "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	admissionapi "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v2/genclient"
	iamapi "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestOrdinaryDeploymentHTTPAdmissionPreservesOptionalAndExplicitKeys(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	store := deploymentAdmissionTestDatabase(t, t.Context(), connection)
	const organization = "optional-key-org"
	project, environment, actor := uuid.New(), uuid.New(), uuid.New()
	runnerID := "runner"
	var storage coreapi.StateStorageConfiguration
	require.NoError(t, storage.FromK8sStorageConfiguration(coreapi.K8sStorageConfiguration{Namespace: "test"}))
	var lock sync.Mutex
	admissions := make([]uuid.UUID, 0)
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/orgs/"+organization+"/deployment-admissions/") {
			if !assert.Equal(t, "Bearer scoped-dp-token", r.Header.Get("Authorization")) {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			id, err := uuid.Parse(strings.TrimPrefix(r.URL.Path, "/orgs/"+organization+"/deployment-admissions/"))
			if !assert.NoError(t, err) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			var body admissionapi.DeploymentAdmissionRequest
			if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&body)) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			assert.Equal(t, actor, body.ActorId)
			assert.Equal(t, project, body.ProjectUuid)
			assert.Equal(t, environment, body.EnvironmentUuid)
			// Intent must already be durable before the remote authority is called.
			tx, err := store.BeginTx(r.Context(), nil)
			if !assert.NoError(t, err) {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			var persisted string
			err = tx.QueryRowContext(r.Context(), `SELECT request_digest FROM deployment_admission_receipts WHERE org_id=$1 AND admission_id=$2 AND disposition='pending'`, organization, id).Scan(&persisted)
			assert.NoError(t, err)
			assert.Equal(t, body.RequestDigest, persisted)
			assert.NoError(t, tx.Rollback())
			lock.Lock()
			admissions = append(admissions, id)
			lock.Unlock()
			assert.NoError(t, json.NewEncoder(w).Encode(admissionapi.DeploymentAdmissionReceipt{AdmissionId: id, ProjectUuid: project, EnvironmentUuid: environment, RequestDigest: body.RequestDigest, Epoch: 1, Mode: string(body.Mode), State: admissionapi.AdmissionStateActive}))
			return
		}
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/envs/environment"):
			assert.NoError(t, json.NewEncoder(w).Encode(coreapi.Environment{Id: "environment", ProjectId: "project", ProjectUuid: &project, Uuid: environment, RunnerId: &runnerID, Status: coreapi.EnvironmentStatusActive}))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/runners/runner"):
			assert.NoError(t, json.NewEncoder(w).Encode(coreapi.Runner{Id: runnerID, StateStorageConfiguration: storage}))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/orgs/"+organization+"/projects/project/envs/environment/module-catalogue":
			assert.NoError(t, json.NewEncoder(w).Encode(coreapi.InternalModuleCatalogue{Modules: []coreapi.InternalModuleCatalogueModule{}, Providers: []coreapi.ModuleProvider{}}))
		default:
			t.Errorf("unexpected Core request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(core.Close)
	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body iamapi.InternalAuthorizeBody
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, actor, body.UserId)
		assert.Equal(t, []iamapi.ResourcePermissionCheck{environmentCheck(environment, PermissionDeploymentWrite)}, body.Checks)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(iam.Close)
	coreClient, err := coreapi.NewClientWithResponses(core.URL)
	require.NoError(t, err)
	iamClient, err := iamapi.NewClientWithResponses(iam.URL)
	require.NoError(t, err)
	tokenFile := t.TempDir() + "/token"
	require.NoError(t, os.WriteFile(tokenFile, []byte("scoped-dp-token"), 0o600))
	service, err := NewOrdinaryDeploymentAdmissionService(store, core.URL, tokenFile, core.Client())
	require.NoError(t, err)
	server := &Server{Database: store, Logger: zap.NewNop(), EventPublisher: &hmessaging.RecordingPublisher{}, ControlPlaneClient: coreClient, IamClient: iamClient, DeploymentAdmissions: service}
	e, err := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{AppName: "optional-admission-key-test", Logger: zap.NewNop(), OpenAPIRawSchema: MustDecodeOpenApiSpec(), OpenAPISkipperFn: SkipGeneratedOpenAPIValidation})
	require.NoError(t, err)
	e.Use(middleware.EchoCtxMiddleware)
	server.MapRoutes(e)
	var rollbackBase uuid.UUID
	call := func(mode, key string) Deployment {
		body := []byte(`{"project_id":"project","env_id":"environment","mode":"` + mode + `","is_dry_run":false,"manifest":{"workloads":{}}}`)
		if mode == "rollback" {
			body = []byte(`{"project_id":"project","env_id":"environment","mode":"rollback","is_dry_run":false,"rollback_to_deployment_id":"` + rollbackBase.String() + `"}`)
		}
		request := httptest.NewRequest(http.MethodPost, "/orgs/"+organization+"/deployments", bytes.NewReader(body))
		request.Header.Set("From", actor.String())
		request.Header.Set("Content-Type", "application/json")
		if key != "" {
			request.Header.Set("Idempotency-Key", key)
		}
		response := httptest.NewRecorder()
		e.ServeHTTP(response, request)
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
		var deployment Deployment
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &deployment))
		return deployment
	}
	first := call("deploy", "")
	rollbackBase = first.Id
	_, err = store.UpdateDeploymentStatusAndOutputs(t.Context(), nil, first.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded})
	require.NoError(t, err)
	second := call("rollback", "")
	require.NotEqual(t, first.Id, second.Id, "unkeyed requests remain separate Deployments")
	_, err = store.UpdateDeploymentStatusAndOutputs(t.Context(), nil, second.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded})
	require.NoError(t, err)
	third := call("deploy", "client-key")
	replayed := call("deploy", "client-key")
	require.Equal(t, third.Id, replayed.Id, "supplied key still replays the ordinary Deployment")
	lock.Lock()
	require.Len(t, admissions, 3, "explicit-key receipt replay adds no new remote effect")
	require.NotEqual(t, admissions[0], admissions[1])
	require.Equal(t, uuid.Version(4), admissions[0].Version())
	require.Equal(t, uuid.Version(4), admissions[1].Version())
	require.Equal(t, uuid.Version(5), admissions[2].Version())
	lock.Unlock()
}
