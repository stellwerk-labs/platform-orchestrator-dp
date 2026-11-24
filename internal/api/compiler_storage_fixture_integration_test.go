//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type compilerFixture struct {
	*queueFixture
	catalogue      cp.InternalModuleCatalogue
	catalogueError *cp.Error
	unassigned     bool
}

func newCompilerFixture(t *testing.T) *compilerFixture {
	t.Helper()
	f := &compilerFixture{queueFixture: newQueueFixture(t, DeploymentQueueConfig{})}
	f.core.set(func(c *queueCore) {
		c.envUUID = uuid.NewMD5(uuid.Nil, []byte("my-env"))
		c.storageNamespace = "some-ns"
	})
	peer := httptest.NewServer(http.HandlerFunc(f.serveCompilerCore))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.ControlPlaneClient = client
	admissions, err := NewOrdinaryDeploymentAdmissionService(f.store, peer.URL, f.tokenFile, peer.Client())
	require.NoError(t, err)
	f.server.DeploymentAdmissions = admissions
	return f
}

func (f *compilerFixture) serveCompilerCore(w http.ResponseWriter, r *http.Request) {
	f.core.mu.Lock()
	catalogue, unavailable, unassigned := f.catalogue, f.catalogueError, f.unassigned
	f.core.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	cataloguePath := "/internal/orgs/" + queueOrg + "/projects/" + queueProject + "/envs/" + queueEnv + "/"
	environmentPath := "/orgs/" + queueOrg + "/projects/" + queueProject + "/envs/" + queueEnv
	switch r.Method + " " + r.URL.Path {
	case http.MethodPost + " " + cataloguePath + "module-catalogue", http.MethodPost + " " + cataloguePath + "module-execution-catalogue":
		if strings.HasSuffix(r.URL.Path, "module-execution-catalogue") && !assert.Equal(f.t, "Bearer queue-dp-token", r.Header.Get("Authorization")) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if strings.HasSuffix(r.URL.Path, "module-execution-catalogue") {
			assert.Empty(f.t, r.Header.Get("From"))
		}
		if unavailable != nil {
			w.WriteHeader(http.StatusConflict)
			assert.NoError(f.t, json.NewEncoder(w).Encode(unavailable))
			return
		}
		assert.NoError(f.t, json.NewEncoder(w).Encode(catalogue))
	case http.MethodPost + " " + environmentPath + "/actions/refresh_runner":
		f.core.set(func(*queueCore) { f.unassigned = false })
		assert.NoError(f.t, json.NewEncoder(w).Encode(cp.RefreshRunnerActionResult{RunnerId: queueRunner}))
	case http.MethodGet + " " + environmentPath:
		if unassigned {
			assert.NoError(f.t, json.NewEncoder(w).Encode(cp.Environment{Id: queueEnv, ProjectId: queueProject, ProjectUuid: &f.core.projectUUID, Uuid: f.core.envUUID, Status: cp.EnvironmentStatusActive}))
			return
		}
		f.core.ServeHTTP(w, r)
	default:
		f.core.ServeHTTP(w, r)
	}
}

func (f *compilerFixture) setCatalogue(catalogue *cp.InternalModuleCatalogue) {
	for i := range catalogue.Modules {
		module := &catalogue.Modules[i]
		module.OrgId = queueOrg
		module.ModuleUuid = uuid.NewSHA1(uuid.NameSpaceURL, []byte(queueOrg+":"+module.Id))
		module.VersionUuid = uuid.NewSHA1(module.ModuleUuid, []byte(module.VersionId))
		module.MigrationGeneration = cp.InternalModuleCatalogueModuleMigrationGenerationV0
		module.SemanticStatus = cp.ModuleVersionSemanticStatusDefault
		module.VerificationStatus = cp.ModuleVerificationStatusUnverified
	}
	f.core.set(func(*queueCore) { f.catalogue = *catalogue })
}

func (f *compilerFixture) create(t *testing.T, body *CreateDeploymentJSONRequestBody) Deployment {
	response := f.deployTyped(body)
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	var dep Deployment
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &dep))
	return dep
}

func (f *compilerFixture) deployTyped(body *CreateDeploymentJSONRequestBody) *httptest.ResponseRecorder {
	return f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", body, f.actor, "")
}

func (f *compilerFixture) stored(t *testing.T, id uuid.UUID) model.CreateDeploymentParams {
	dep, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, id, dep.Id)
	require.Equal(t, f.core.envUUID, dep.DeploymentEnvUuid)
	require.Equal(t, model.DeploymentStatusExecuting, dep.Status)
	return model.CreateDeploymentParams{DeploymentEnvUuid: dep.DeploymentEnvUuid, Mode: dep.Mode, Manifest: manifest, Graph: graph, Tofu: tofu,
		EncryptedOutputsRecipient: dep.EncryptedOutputsRecipient, EncryptedLogsRecipient: dep.EncryptedLogsRecipient}
}

func (f *compilerFixture) completeCompilerDeployment(t *testing.T, dep Deployment) {
	request := f.request(http.MethodPost, "/orgs/"+queueOrg+"/deployments/"+dep.Id.String()+"/results", DeploymentResultsUpdateBody{Status: Success}, f.actor, "")
	request.Header.Set("X-Deployment-Token", util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, queueOrg, dep.Id.String()))
	response := f.serve(request)
	require.Equal(t, http.StatusNoContent, response.Code, response.Body.String())
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, dep.Id))
}
