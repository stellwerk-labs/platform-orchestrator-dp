package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stellwerk-labs/golib/hecho"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	iam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func rolloutHTTPTest(t *testing.T, assigned bool, permission string, upstream http.HandlerFunc) (*echo.Echo, uuid.UUID, uuid.UUID) {
	t.Helper()
	actor, environment := uuid.New(), uuid.New()
	core := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", observationJSONContentType)
		runner := "edge-site"
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/actions/refresh_runner") {
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]string{"runner_id": runner}))
			return
		}
		env := cp.Environment{Id: "env", ProjectId: "project", Uuid: environment}
		if assigned {
			env.RunnerId = &runner
		}
		assert.NoError(t, json.NewEncoder(w).Encode(env))
	}))
	t.Cleanup(core.Close)
	authority := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body iam.InternalAuthorizeBody
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, actor, body.UserId)
		if permission != "" && body.Checks[0].Permission != permission {
			w.Header().Set("Content-Type", observationJSONContentType)
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"details":{"reason":"denied"}}`))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(authority.Close)
	adapter := httptest.NewServer(upstream)
	t.Cleanup(adapter.Close)
	coreClient, err := cp.NewClientWithResponses(core.URL)
	require.NoError(t, err)
	iamClient, err := iam.NewClientWithResponses(authority.URL)
	require.NoError(t, err)
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("adapter-secret"), 0o600))
	s := &Server{UiHostUrl: "https://console.invalid", ControlPlaneClient: coreClient, IamClient: iamClient, Logger: zap.NewNop()}
	service, err := NewRunnerRolloutService(s, adapter.URL, tokenFile, "org", "edge-site")
	require.NoError(t, err)
	e := echo.New()
	e.HTTPErrorHandler = hecho.CustomHTTPErrorHandler
	service.MapRoutes(e)
	return e, actor, environment
}

func callRolloutHTTP(e *echo.Echo, actor uuid.UUID, method, suffix, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "/orgs/org/projects/project/envs/env/runner-rollouts"+suffix, strings.NewReader(body))
	request.Header.Set("From", actor.String())
	request.Header.Set("Content-Type", observationJSONContentType)
	request.Header.Set("Idempotency-Key", "browser-key")
	request.Header.Set("X-Rollout-Actor", "untrusted-browser-actor")
	response := httptest.NewRecorder()
	e.ServeHTTP(response, request)
	return response
}

func TestRunnerRolloutsFilterListAndRefuseCrossEnvironmentActions(t *testing.T) {
	var environment uuid.UUID
	id := uuid.NewString()
	other := uuid.NewString()
	var mutations atomic.Int32
	e, actor, env := rolloutHTTPTest(t, true, "", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", observationJSONContentType)
		if r.Method != http.MethodGet {
			mutations.Add(1)
		}
		if r.URL.Path == "/api/runner-rollouts" {
			assert.NoError(t, json.NewEncoder(w).Encode([]runnerRolloutView{{ID: id, EnvironmentID: environment.String()}, {ID: other, EnvironmentID: uuid.NewString()}}))
			return
		}
		assert.NoError(t, json.NewEncoder(w).Encode(runnerRolloutView{ID: other, EnvironmentID: uuid.NewString()}))
	})
	environment = env
	listed := callRolloutHTTP(e, actor, http.MethodGet, "", "")
	require.Equal(t, http.StatusOK, listed.Code)
	var runs []runnerRolloutView
	require.NoError(t, json.Unmarshal(listed.Body.Bytes(), &runs))
	require.Len(t, runs, 1)
	require.Equal(t, id, runs[0].ID)
	require.Equal(t, http.StatusNotFound, callRolloutHTTP(e, actor, http.MethodGet, "/"+other, "").Code)
	require.Equal(t, http.StatusNotFound, callRolloutHTTP(e, actor, http.MethodPost, "/"+other+"/approve", `{"subject":"exact-plan"}`).Code)
	require.Zero(t, mutations.Load())
}

func TestRunnerRolloutCreationIsRetired(t *testing.T) {
	var calls atomic.Int32
	e, actor, _ := rolloutHTTPTest(t, true, "", func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	response := callRolloutHTTP(e, actor, http.MethodPost, "", `{"value":"legacy"}`)
	require.Equal(t, http.StatusGone, response.Code)
	require.Zero(t, calls.Load())
}

func TestRunnerRolloutWriteRechecksIAMBeforeCallingAdapter(t *testing.T) {
	var calls atomic.Int32
	e, actor, _ := rolloutHTTPTest(t, true, PermissionDeploymentRead, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	response := callRolloutHTTP(e, actor, http.MethodPost, "/"+uuid.NewString()+"/cancel", `{}`)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Zero(t, calls.Load())
}

func TestRunnerRolloutReadsDoNotAssignAnUnconfiguredEnvironment(t *testing.T) {
	var calls atomic.Int32
	e, actor, _ := rolloutHTTPTest(t, false, "", func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	require.Equal(t, http.StatusConflict, callRolloutHTTP(e, actor, http.MethodGet, "/catalog", "").Code)
	require.Equal(t, http.StatusConflict, callRolloutHTTP(e, actor, http.MethodGet, "", "").Code)
	require.Zero(t, calls.Load())
}

func TestRunnerRolloutAdapterFailureDoesNotExposeResponseBody(t *testing.T) {
	e, actor, _ := rolloutHTTPTest(t, true, "", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("private-canary"))
	})
	response := callRolloutHTTP(e, actor, http.MethodGet, "", "")
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.NotContains(t, response.Body.String(), "private-canary")
}

func TestRunnerRolloutCatalogRequiresTheConfiguredSite(t *testing.T) {
	e, actor, _ := rolloutHTTPTest(t, true, "", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", observationJSONContentType)
		_, _ = w.Write([]byte(`{"site":{"org_id":"another-org","runner_id":"edge-site"},"tools":[]}`))
	})
	require.Equal(t, http.StatusServiceUnavailable, callRolloutHTTP(e, actor, http.MethodGet, "/catalog", "").Code)
}

func TestRunnerRolloutRejectsCrossOriginMutation(t *testing.T) {
	var mutations atomic.Int32
	var environment uuid.UUID
	id := uuid.NewString()
	e, actor, env := rolloutHTTPTest(t, true, "", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mutations.Add(1)
		}
		w.Header().Set("Content-Type", observationJSONContentType)
		_ = json.NewEncoder(w).Encode(runnerRolloutView{ID: id, EnvironmentID: environment.String()})
	})
	environment = env
	request := httptest.NewRequest(http.MethodPost, "/orgs/org/projects/project/envs/env/runner-rollouts/"+id+"/cancel", strings.NewReader(`{}`))
	request.Header.Set("From", actor.String())
	request.Header.Set("Origin", "https://untrusted.invalid")
	request.Header.Set("Content-Type", observationJSONContentType)
	response := httptest.NewRecorder()
	e.ServeHTTP(response, request)
	require.Equal(t, http.StatusForbidden, response.Code)
	require.Zero(t, mutations.Load())
}
