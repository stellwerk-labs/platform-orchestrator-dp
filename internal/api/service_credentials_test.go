package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdmissionAndCatalogueReloadCredentialsWithoutChangingRequestIdentity(t *testing.T) {
	name := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(name, []byte("initial-token"), 0o600))
	var current atomic.Value
	current.Store("initial-token")
	var requests atomic.Int64
	admission := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Equal(t, "Bearer "+current.Load().(string), r.Header.Get("Authorization"))
		assert.Empty(t, r.Header.Get("From"))
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/module-execution-catalogue") {
			_, _ = w.Write([]byte(`{"modules":[],"providers":[]}`))
			return
		}
		assert.Contains(t, r.URL.Path, admission.String())
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(server.Close)
	service, err := NewOrdinaryDeploymentAdmissionService(nil, server.URL, name, server.Client())
	require.NoError(t, err)
	for _, token := range []string{"initial-token", "rotated-token"} {
		replaceServiceToken(t, name, token)
		current.Store(token)
		_, err = service.Core.AuthorizeDeploymentAdmissionWithResponse(t.Context(), "org", admission,
			platformorchestratorcp.DeploymentAdmissionRequest{}, func(_ context.Context, r *http.Request) error {
				r.Header.Set("From", "forged-identity")
				return nil
			}, service.bearer())
		require.NoError(t, err)
		_, err = service.ExecutionCatalogueClient().GenerateInternalExecutionCatalogueWithResponse(t.Context(), "org", "project", "env", nil,
			platformorchestratorcp.InternalExecutionCatalogueGenerateBody{})
		require.NoError(t, err)
	}
	require.EqualValues(t, 4, requests.Load(), "rotation does not retry a request")
}

func TestCredentialReloadFailureDoesNotUseOldTokenOrSendRequest(t *testing.T) {
	for _, replacement := range []string{"", "SECRET_RELOAD_CANARY invalid", strings.Repeat("x", maxExternalDeploymentCredentialBytes+1)} {
		t.Run(fmt.Sprintf("bytes-%d", len(replacement)), func(t *testing.T) {
			name := filepath.Join(t.TempDir(), "token")
			require.NoError(t, os.WriteFile(name, []byte("initial-token"), 0o600))
			var requests atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
			t.Cleanup(server.Close)
			service, err := NewOrdinaryDeploymentAdmissionService(nil, server.URL, name, server.Client())
			require.NoError(t, err)
			replaceServiceToken(t, name, replacement)
			_, err = service.Core.AuthorizeDeploymentAdmissionWithResponse(t.Context(), "org", uuid.New(),
				platformorchestratorcp.DeploymentAdmissionRequest{}, service.bearer())
			require.ErrorIs(t, err, ErrDeploymentAdmissionUnavailable)
			require.NotContains(t, err.Error(), "SECRET_RELOAD_CANARY")
			_, err = service.ExecutionCatalogueClient().GenerateInternalExecutionCatalogueWithResponse(t.Context(), "org", "project", "env", nil,
				platformorchestratorcp.InternalExecutionCatalogueGenerateBody{})
			require.ErrorIs(t, err, ErrDeploymentAdmissionUnavailable)
			require.Zero(t, requests.Load())
			require.NoError(t, os.Remove(name))
			_, err = service.ExecutionCatalogueClient().GenerateInternalExecutionCatalogueWithResponse(t.Context(), "org", "project", "env", nil,
				platformorchestratorcp.InternalExecutionCatalogueGenerateBody{})
			require.ErrorIs(t, err, ErrDeploymentAdmissionUnavailable)
			require.Zero(t, requests.Load())
		})
	}
}

func TestExternalGrantReloadsCredentialsAndFailsClosed(t *testing.T) {
	name := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(name, []byte("initial-token"), 0o600))
	received := make(chan string, 3)
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		received <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusForbidden)
	}))
	t.Cleanup(server.Close)
	baseURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	client := CoreExternalDeploymentGrantClient{BaseURL: baseURL, HTTPClient: server.Client(), BearerToken: "stale-token", BearerTokenFile: name}
	claims := plugincontext.Claims{OrgID: "org", PluginID: "dev.example.rollouts", ActorID: uuid.NewString()}
	request := ExternalDeploymentGrantRequest{OperationRef: uuid.NewString()}
	for _, token := range []string{"initial-token", "rotated-token"} {
		replaceServiceToken(t, name, token)
		_, err := client.AuthorizeExternalDeployment(t.Context(), claims, request)
		require.ErrorIs(t, err, ErrExternalDeploymentUnauthorized)
	}
	require.Equal(t, "Bearer initial-token", <-received)
	require.Equal(t, "Bearer rotated-token", <-received)
	replaceServiceToken(t, name, "SECRET_RELOAD_CANARY invalid")
	_, err = client.AuthorizeExternalDeployment(t.Context(), claims, request)
	require.ErrorIs(t, err, ErrExternalDeploymentUnavailable)
	require.NotContains(t, err.Error(), "SECRET_RELOAD_CANARY")
	require.EqualValues(t, 2, requests.Load())
}

func TestConcurrentCredentialRotationReadsOnlyCompleteReplacements(t *testing.T) {
	name := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(name, []byte("token-a"), 0o600))
	service, err := NewOrdinaryDeploymentAdmissionService(nil, "https://gateway.example.test", name, nil)
	require.NoError(t, err)
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 40 {
				request := httptest.NewRequest(http.MethodPost, "https://gateway.example.test", nil)
				if assert.NoError(t, service.bearer()(t.Context(), request)) {
					assert.Contains(t, []string{"Bearer token-a", "Bearer token-b"}, request.Header.Get("Authorization"))
				}
			}
		})
	}
	for i := range 40 {
		replaceServiceToken(t, name, []string{"token-a", "token-b"}[i%2])
	}
	readers.Wait()
}

func replaceServiceToken(t *testing.T, name, token string) {
	t.Helper()
	require.NoError(t, os.WriteFile(name+".new", []byte(token), 0o600))
	require.NoError(t, os.Rename(name+".new", name))
}
