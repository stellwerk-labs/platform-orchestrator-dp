//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

func TestLegacyDeploymentAccessUsesStoredProvenance(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.RunnerTokenSalt = uuid.NewString()
	dep := f.mustDeploy(queueBody(nil))
	require.Nil(t, f.server.RetainedPlans, "receipt route must not depend on retained-plan enablement")
	// The receipt must not consult mutable Core Runner metadata or IAM.
	f.server.ControlPlaneClient, f.server.IamClient = nil, nil
	f.exec(`UPDATE deployments SET runner_id='R' WHERE id=$1`, dep.Id)
	path := "/v1/orgs/" + queueOrg + "/runners/R/deployments/" + dep.Id.String() + "/legacy-access"
	call := func(org, target string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, target, nil)
		r.Header.Set("X-Deployment-Token", util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, org, dep.Id.String()))
		w := httptest.NewRecorder()
		f.echo.ServeHTTP(w, r)
		return w
	}
	w := call(queueOrg, path)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	require.Contains(t, w.Header().Get("Content-Type"), "application/json")
	require.LessOrEqual(t, w.Body.Len(), 4096)
	var access legacyDeploymentAccess
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &access))
	require.Equal(t, legacyDeploymentAccess{Legacy: true, OrganizationID: queueOrg, DeploymentID: dep.Id, RunnerID: "R", EnvironmentUUID: f.core.envUUID}, access)
	require.JSONEq(t, `{"legacy":true,"organization_id":"`+queueOrg+`","deployment_id":"`+dep.Id.String()+`","runner_id":"R","environment_uuid":"`+f.core.envUUID.String()+`"}`, w.Body.String())

	for _, target := range []struct{ org, runner string }{
		{"foreign", "R"}, {queueOrg, "foreign"},
	} {
		foreignPath := "/v1/orgs/" + target.org + "/runners/" + target.runner + "/deployments/" + dep.Id.String() + "/legacy-access"
		require.Equal(t, http.StatusNotFound, call(target.org, foreignPath).Code)
	}
	missing := uuid.NewString()
	r := httptest.NewRequest(http.MethodGet, "/v1/orgs/"+queueOrg+"/runners/R/deployments/"+missing+"/legacy-access", nil)
	r.Header.Set("X-Deployment-Token", util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, queueOrg, missing))
	missingResponse := httptest.NewRecorder()
	f.echo.ServeHTTP(missingResponse, r)
	require.Equal(t, http.StatusNotFound, missingResponse.Code)
	target, err := json.Marshal(manualNativeTarget(t))
	require.NoError(t, err)
	for _, raw := range [][]byte{target, []byte(`{}`)} {
		f.exec(`UPDATE deployments SET native_target=$2 WHERE id=$1`, dep.Id, raw)
		denied := call(queueOrg, path)
		require.Equal(t, http.StatusNotFound, denied.Code)
		require.Equal(t, missingResponse.Body.String(), denied.Body.String(), "Native and absent rows must have the same refusal")
	}
}

func TestLegacyDeploymentAccessDatabaseFailureIsUnavailable(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.RunnerTokenSalt = uuid.NewString()
	dep := f.mustDeploy(queueBody(nil))
	path := "/v1/orgs/" + queueOrg + "/runners/" + queueRunner + "/deployments/" + dep.Id.String() + "/legacy-access"
	f.exec(`ALTER TABLE deployments RENAME TO unavailable_deployments`)
	for _, closeStore := range []bool{false, true} {
		if closeStore {
			require.NoError(t, f.store.Close())
		}
		r := httptest.NewRequest(http.MethodGet, path, nil)
		r.Header.Set("X-Deployment-Token", util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, queueOrg, dep.Id.String()))
		w := httptest.NewRecorder()
		f.echo.ServeHTTP(w, r)
		require.Equal(t, http.StatusServiceUnavailable, w.Code)
		require.NotContains(t, w.Body.String(), "legacy")
	}
}
