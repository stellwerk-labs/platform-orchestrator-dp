//go:build integration

package envdestroyhandler

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Actual deletion handler and PostgreSQL, with a typed Core contract peer.
// The retained Native base is a storage fixture, not proof of site execution.
func TestNativeDestroyHandlerUsesSystemAdmissionAndDoesNotQueueOnRedelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "native_destroy_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.ExecContext(context.WithoutCancel(ctx), "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	store, err := model.NewDatabaser(ctx, zap.NewNop(), connection+" search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	project, environment, registration := uuid.New(), uuid.New(), uuid.New()
	runner := cp.Runner{Id: "runner", OrgId: "org", Uuid: &registration}
	protocols, capabilities := []string{"stellwerk.pipeline/v2", "stellwerk.pipeline.plan-apply/v2"}, []string{"pipeline.recipe.v1"}
	require.NoError(
		t,
		runner.RunnerConfiguration.FromNativeAgentRunnerConfiguration(
			cp.NativeAgentRunnerConfiguration{
				Type:               "native-agent",
				MaxParallelJobs:    2,
				SupportedProtocols: &protocols,
				Capabilities:       &capabilities,
				NativeExecution:    &cp.RunnerExecutionCatalogue{ContractVersion: cp.StellwerkRunnerCataloguev1, RuntimeVersion: cp.StellwerkRunnerRolloutv2, ToolsDigest: strings.Repeat("a", 64), Tools: []cp.RunnerExecutionTool{}},
			},
		),
	)
	require.NoError(t, runner.StateStorageConfiguration.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: "state"}))
	raw, err := json.Marshal(runner.StateStorageConfiguration)
	require.NoError(t, err)
	backend, digest, err := cp.StateStorageConfigurationDigest(raw)
	require.NoError(t, err)
	target := &cp.NativeExecutionTargetReference{RunnerId: runner.Id, RunnerUuid: registration, RuntimeVersion: cp.NativeTargetRuntimeV2, PipelineVersion: cp.NativeTargetPipelineV2, ToolsDigest: strings.Repeat("a", 64), StateBackendType: backend, StateConfigurationDigest: digest}
	eligible := true
	env := cp.Environment{Id: "environment", ProjectId: "project", ProjectUuid: &project, Uuid: environment, RunnerId: &runner.Id, NativeFirstEligible: &eligible, Status: cp.EnvironmentStatusDeleting, DeletionPolicy: &cp.EnvironmentDeletionPolicy{}}
	admissions := make(chan cp.DeploymentAdmissionRequest, 2)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/orgs/org/projects/project/envs/environment":
			assert.NoError(t, json.NewEncoder(w).Encode(env))
		case r.Method == http.MethodGet && r.URL.Path == "/orgs/org/runners/runner":
			assert.NoError(t, json.NewEncoder(w).Encode(runner))
		case strings.HasSuffix(r.URL.Path, "/module-execution-catalogue"):
			assert.NoError(t, json.NewEncoder(w).Encode(cp.InternalExecutionCatalogue{Modules: []cp.InternalExecutionCatalogueModule{}, Providers: []cp.ModuleProvider{}}))
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/orgs/org/deployment-admissions/"):
			var request cp.DeploymentAdmissionRequest
			if !assert.NoError(t, json.NewDecoder(r.Body).Decode(&request)) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			admissions <- request
			id, err := uuid.Parse(strings.TrimPrefix(r.URL.Path, "/orgs/org/deployment-admissions/"))
			if !assert.NoError(t, err) {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			assert.NoError(t, json.NewEncoder(w).Encode(cp.DeploymentAdmissionReceipt{AdmissionId: id, ProjectUuid: project, EnvironmentUuid: environment, RequestDigest: request.RequestDigest, Mode: string(request.Mode), Epoch: 1, State: cp.AdmissionStateActive}))
		default:
			assert.NoError(t, json.NewEncoder(w).Encode(env))
		}
	}))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	token := t.TempDir() + "/token"
	require.NoError(t, os.WriteFile(token, []byte("owned-destroy-token"), 0600))
	service, err := api.NewOrdinaryDeploymentAdmissionService(store, peer.URL, token, http.DefaultClient)
	require.NoError(t, err)
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	base, err := store.CreateDeployment(
		ctx,
		tx,
		"org",
		"project",
		"environment",
		model.CreateDeploymentParams{
			NativeTarget:      target,
			DeploymentEnvUuid: environment,
			Mode:              model.DeploymentModeDeploy,
			CreatedBy:         uuid.New(),
			RunnerId:          runner.Id,
			Manifest:          model.EncodedDeploymentManifest(`{"workloads":{}}`),
			Graph:             model.EncodedDeploymentGraph(`{"nodes":{},"edges":{},"workloads":{},"shared_resources":{},"leaf_resources":{}}`),
			Tofu:              model.RawTofu("terraform {}"),
			RunnerLogLevel:    "info",
		},
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, nil, base.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded})
	require.NoError(t, err)
	handler := New(client, store, &hmessaging.RecordingPublisher{}, func(context.Context, string) error { return nil }, service)
	require.NoError(t, handler.handleInner(ctx, zap.NewNop(), "org", "project", "environment", environment))
	require.NoError(t, handler.handleInner(ctx, zap.NewNop(), "org", "project", "environment", environment))
	require.Len(t, admissions, 1)
	request := <-admissions
	require.Equal(t, userid.InternalSystemUuid, request.ActorId)
	require.Equal(t, string(model.DeploymentModeDestroy), string(request.Mode))
	require.Equal(t, target, request.NativeTarget)
	require.Nil(t, request.Pipeline, "the fixed builtin destroy is not a published project recipe")
	tx, err = store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var deployments, queued int
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM deployments`).Scan(&deployments))
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM environment_deployment_queue`).Scan(&queued))
	require.Equal(t, 2, deployments)
	require.Zero(t, queued)
}
