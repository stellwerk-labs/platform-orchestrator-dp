package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOrdinaryDeploymentAdmissionServiceRejectsUnsafeConfiguration(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	require.NoError(t, os.WriteFile(tokenFile, []byte("scoped-token\n"), 0o600))
	for _, rawURL := range []string{"ftp://gateway.example.test", "https://user@gateway.example.test", "https://gateway.example.test?target=other", "https://gateway.example.test#fragment"} {
		t.Run(rawURL, func(t *testing.T) {
			_, err := NewOrdinaryDeploymentAdmissionService(nil, rawURL, tokenFile, nil)
			require.Error(t, err)
		})
	}
	_, err := NewOrdinaryDeploymentAdmissionService(nil, "https://gateway.example.test", "relative-token", nil)
	require.ErrorContains(t, err, "absolute path")
	require.NoError(t, os.WriteFile(tokenFile, []byte("two tokens"), 0o600))
	_, err = NewOrdinaryDeploymentAdmissionService(nil, "https://gateway.example.test", tokenFile, nil)
	require.ErrorContains(t, err, "one non-empty token")
}

func TestDeploymentAdmissionClientDoesNotFollowRedirectsOrReadUnboundedBodies(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	require.NoError(t, os.WriteFile(tokenFile, []byte("scoped-token"), 0o600))
	var redirected atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { redirected.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL)
		writer.WriteHeader(http.StatusTemporaryRedirect)
	}))
	service, err := NewOrdinaryDeploymentAdmissionService(nil, redirect.URL, tokenFile, redirect.Client())
	require.NoError(t, err)
	response, err := service.Core.AuthorizeDeploymentAdmissionWithResponse(
		context.Background(), "org", uuid.New(), platformorchestratorcp.DeploymentAdmissionRequest{},
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusTemporaryRedirect, response.StatusCode())
	require.Zero(t, redirected.Load())
	redirect.Close()

	oversized := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(strings.Repeat(" ", maxDeploymentAdmissionResponseBytes+1)))
	}))
	defer oversized.Close()
	service, err = NewOrdinaryDeploymentAdmissionService(nil, oversized.URL, tokenFile, oversized.Client())
	require.NoError(t, err)
	_, err = service.Core.AuthorizeDeploymentAdmissionWithResponse(
		context.Background(), "org", uuid.New(), platformorchestratorcp.DeploymentAdmissionRequest{},
	)
	require.ErrorIs(t, err, errDeploymentAdmissionResponseTooLarge)
}

// This exercises the real generated client's wire behavior, not catalogue policy.
func TestExecutionCatalogueClientUsesAuthenticatedAdmissionOrigin(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	require.NoError(t, os.WriteFile(tokenFile, []byte("scoped-token"), 0o600))
	var received atomic.Int64
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		assert.Equal(t, "/internal/orgs/org/projects/project/envs/development/module-execution-catalogue", r.URL.Path)
		assert.Equal(t, "Bearer scoped-token", r.Header.Get("Authorization"))
		assert.Empty(t, r.Header.Get("From"), "only the authenticated gateway may supply this identity")
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"modules":[],"providers":[]}`))
		assert.NoError(t, err)
	}))
	defer gateway.Close()
	service, err := NewOrdinaryDeploymentAdmissionService(nil, gateway.URL, tokenFile, gateway.Client())
	require.NoError(t, err)
	require.NotNil(t, service.ExecutionCatalogueClient())
	response, err := service.ExecutionCatalogueClient().GenerateInternalExecutionCatalogueWithResponse(t.Context(), "org", "project", "development", nil, platformorchestratorcp.InternalExecutionCatalogueGenerateBody{})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode())
	require.Equal(t, int64(1), received.Load())
	var disabled *OrdinaryDeploymentAdmissionService
	require.Nil(t, disabled.ExecutionCatalogueClient())
}
