//go:build integration

package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"filippo.io/age"
	"github.com/google/uuid"
	vaultapi "github.com/hashicorp/vault/api"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestNativeDirectBundleRefusesBeforeMaterialConstruction(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.RunnerTokenSalt = "owned-material-fixture"
	dep := f.mustDeploy(queueBody(nil))
	request := GetDeploymentBundleRequestObject{OrgId: queueOrg, DeploymentId: dep.Id, Params: GetDeploymentBundleParams{
		XDeploymentToken: util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, queueOrg, dep.Id.String()),
	}}
	legacy, err := f.server.GetDeploymentBundle(f.ctx, request)
	require.NoError(t, err)
	require.IsType(t, GetDeploymentBundle200ApplicationxGzipResponse{}, legacy)
	var calls atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		f.core.ServeHTTP(w, r)
	}))
	t.Cleanup(peer.Close)
	f.server.ControlPlaneClient, err = cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.DeploymentAdmissions, err = NewOrdinaryDeploymentAdmissionService(f.store, peer.URL, f.tokenFile, http.DefaultClient)
	require.NoError(t, err)
	target, err := json.Marshal(manualNativeTarget(t))
	require.NoError(t, err)
	for _, raw := range [][]byte{target, []byte(`{}`)} {
		f.exec(`UPDATE deployments SET native_target=$2 WHERE id=$1`, dep.Id, raw)
		response, err := f.server.GetDeploymentBundle(f.ctx, request)
		require.NoError(t, err)
		require.IsType(t, GetDeploymentBundle400JSONResponse{}, response)
		require.Zero(t, calls.Load())
	}
	missing := request
	missing.DeploymentId = uuid.New()
	missing.Params.XDeploymentToken = util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, queueOrg, missing.DeploymentId.String())
	response, err := f.server.GetDeploymentBundle(f.ctx, missing)
	require.NoError(t, err)
	require.IsType(t, GetDeploymentBundle404JSONResponse{}, response)
	require.Zero(t, calls.Load())
}

func TestNativeDirectKeyRefusesBeforeRealVaultAccess(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.RunnerTokenSalt = "owned-material-fixture"
	f.server.Vault = managedLogVault(t)
	service := &RetainedPlanService{Server: f.server}
	f.server.RetainedPlans = service
	env, err := f.coreClient.GetEnvironmentWithResponse(f.ctx, queueOrg, queueProject, queueEnv)
	require.NoError(t, err)
	operation := uuid.New()
	plan, err := service.CreatePlan(f.ctx, f.actor, RetainedPlanRequest{OrgID: queueOrg, ProjectID: queueProject, EnvID: queueEnv,
		Environment: *env.JSON200, Manifest: DeploymentManifest{}, Backend: "kubernetes", Tool: "opentofu"}, operation)
	require.NoError(t, err)
	snapshot, err := json.Marshal(retainedSnapshot{Binding: plan.Binding, OS: "linux", Arch: "amd64",
		ToolSHA256: strings.Repeat("a", 64), Archive: []byte("owned retained archive"), Review: json.RawMessage(`{"planned_values":{"root_module":{}}}`)})
	require.NoError(t, err)
	recipient, err := age.ParseX25519Recipient(plan.Recipient)
	require.NoError(t, err)
	var ciphertext bytes.Buffer
	writer, err := age.Encrypt(&ciphertext, recipient)
	require.NoError(t, err)
	_, err = writer.Write(snapshot)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	encoded := base64.StdEncoding.EncodeToString(ciphertext.Bytes())
	require.NoError(t, f.server.ApplyRunnerDeploymentResult(f.ctx, queueOrg, queueRunner, plan.SourceDeploymentID, uuid.NewString(), DeploymentResultsUpdateBody{Status: Success, Outputs: &encoded}))
	apply, err := service.ApplyPlan(f.ctx, f.actor, queueOrg, operation, "owned-subject", "owned-key")
	require.NoError(t, err)

	upstream, err := url.Parse(os.Getenv("COST_POLICY_TEST_VAULT_ADDR"))
	require.NoError(t, err)
	// #nosec G704 -- The operator-supplied fixture URL is the actual isolated Vault under test.
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	var calls atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(peer.Close)
	config := vaultapi.DefaultConfig()
	config.Address, config.MaxRetries = peer.URL, 0
	client, err := vaultapi.NewClient(config)
	require.NoError(t, err)
	client.SetToken(os.Getenv("COST_POLICY_TEST_VAULT_TOKEN"))
	f.server.Vault = vault.NewVaultClient(client, zap.NewNop())
	f.server.ControlPlaneClient = nil
	f.echo.GET("/v1/orgs/:orgId/runners/:runnerId/deployments/:deploymentId/retained-plan-key", service.HandleKey)
	path := "/v1/orgs/" + queueOrg + "/runners/" + queueRunner + "/deployments/" + apply.Id.String() + "/retained-plan-key"
	request := f.request(http.MethodGet, path, nil, f.actor, "")
	request.Header.Set("X-Deployment-Token", util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, queueOrg, apply.Id.String()))
	legacy := f.serve(request)
	require.Equal(t, http.StatusOK, legacy.Code)
	var body struct {
		Identity string `json:"identity"`
	}
	require.NoError(t, json.Unmarshal(legacy.Body.Bytes(), &body))
	identity, err := age.ParseX25519Identity(body.Identity)
	if err != nil {
		t.Fatal("authorized fixture key must parse")
	}
	require.Equal(t, plan.Recipient, identity.Recipient().String())
	require.Positive(t, calls.Load(), "positive control must reach the actual Vault")
	calls.Store(0)
	for _, scope := range []struct {
		org, runner string
		deployment  uuid.UUID
	}{
		{"foreign-org", queueRunner, apply.Id},
		{queueOrg, "foreign-runner", apply.Id},
		{queueOrg, queueRunner, uuid.New()},
	} {
		path := "/v1/orgs/" + scope.org + "/runners/" + scope.runner + "/deployments/" + scope.deployment.String() + "/retained-plan-key"
		request := f.request(http.MethodGet, path, nil, f.actor, "")
		request.Header.Set("X-Deployment-Token", util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, scope.org, scope.deployment.String()))
		require.Equal(t, http.StatusNotFound, f.serve(request).Code)
		require.Zero(t, calls.Load())
	}
	target, err := json.Marshal(manualNativeTarget(t))
	require.NoError(t, err)
	for _, raw := range [][]byte{target, []byte(`{}`)} {
		f.exec(`UPDATE deployments SET native_target=$2 WHERE id=$1`, apply.Id, raw)
		request := f.request(http.MethodGet, path, nil, f.actor, "")
		request.Header.Set("X-Deployment-Token", util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, queueOrg, apply.Id.String()))
		refused := f.serve(request)
		require.Equal(t, http.StatusNotFound, refused.Code)
		if strings.Contains(refused.Body.String(), "AGE-SECRET-KEY") {
			t.Fatal("refusal disclosed a private key")
		}
		require.Zero(t, calls.Load())
	}
	// The same retained artifact also cannot use the legacy bundle route.
	response, err := f.server.GetDeploymentBundle(f.ctx, GetDeploymentBundleRequestObject{OrgId: queueOrg, DeploymentId: apply.Id,
		Params: GetDeploymentBundleParams{XDeploymentToken: util.GenerateHashedRunnerToken(f.server.RunnerTokenSalt, queueOrg, apply.Id.String())}})
	require.NoError(t, err)
	require.IsType(t, GetDeploymentBundle400JSONResponse{}, response)
	require.Zero(t, calls.Load())
	var stored *model.DeploymentSummary
	stored, _, _, _, err = f.store.GetDeployment(f.ctx, nil, queueOrg, apply.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentStatusExecuting, stored.Status)
}
