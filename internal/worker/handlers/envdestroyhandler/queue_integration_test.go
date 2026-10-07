//go:build integration

package envdestroyhandler

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratorgraph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

// destroyCore serves the Core calls of an Environment deletion and records every change it is asked for.
type destroyCore struct {
	t       *testing.T
	envUUID uuid.UUID
	mu      sync.Mutex
	changes []string
}

func (c *destroyCore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	runner := "runner"
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/orgs/org/projects/project/envs/environment":
		assert.NoError(c.t, json.NewEncoder(w).Encode(platformorchestratorcp.Environment{Id: "environment", ProjectId: "project", Uuid: c.envUUID, RunnerId: &runner, Status: platformorchestratorcp.EnvironmentStatusDeleting}))
	case r.Method == http.MethodGet && r.URL.Path == "/orgs/org/runners/runner":
		var storage platformorchestratorcp.StateStorageConfiguration
		assert.NoError(c.t, storage.FromK8sStorageConfiguration(platformorchestratorcp.K8sStorageConfiguration{Namespace: "test"}))
		assert.NoError(c.t, json.NewEncoder(w).Encode(platformorchestratorcp.Runner{Id: runner, StateStorageConfiguration: storage}))
	case r.Method == http.MethodPost && r.URL.Path == "/internal/orgs/org/projects/project/envs/environment/module-catalogue":
		assert.NoError(c.t, json.NewEncoder(w).Encode(platformorchestratorcp.InternalModuleCatalogue{Modules: []platformorchestratorcp.InternalModuleCatalogueModule{}, Providers: []platformorchestratorcp.ModuleProvider{}}))
	default:
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		c.changes = append(c.changes, r.Method+" "+r.URL.Path+" "+string(body))
		c.mu.Unlock()
		assert.NoError(c.t, json.NewEncoder(w).Encode(platformorchestratorcp.Environment{Id: "environment", ProjectId: "project", Uuid: c.envUUID, Status: platformorchestratorcp.EnvironmentStatusDeleting}))
	}
}

func (c *destroyCore) recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.changes...)
}

func TestDeletionCancelsQueuedDeploymentsAndDestroyWaitsInsteadOfFailing(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "queue_destroy_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.ExecContext(context.WithoutCancel(ctx), "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	store, err := model.NewDatabaser(ctx, zap.NewNop(), connection+" search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	core := &destroyCore{t: t, envUUID: uuid.New()}
	server := httptest.NewServer(core)
	t.Cleanup(server.Close)
	client, err := platformorchestratorcp.NewClientWithResponses(server.URL)
	require.NoError(t, err)
	handler := New(client, store, &hmessaging.RecordingPublisher{}, func(context.Context, string) error { return nil }, nil)

	graph, err := json.Marshal(&platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]{})
	require.NoError(t, err)
	create := func(mode model.DeploymentMode) *model.DeploymentSummary {
		t.Helper()
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		dep, err := store.CreateDeployment(ctx, tx, "org", "project", "environment", model.CreateDeploymentParams{DeploymentEnvUuid: core.envUUID, Mode: mode, CreatedBy: uuid.New(),
			RunnerId: "runner", RunnerLogLevel: "info", Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`), Graph: model.EncodedDeploymentGraph(graph), Tofu: model.RawTofu("terraform {}")})
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		return dep
	}
	succeed := func(id uuid.UUID) {
		t.Helper()
		_, err := store.UpdateDeploymentStatusAndOutputs(ctx, nil, id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded})
		require.NoError(t, err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, query, args...)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}
	destroy := func() error {
		return handler.handleInner(ctx, zaptest.NewLogger(t), "org", "project", "environment", core.envUUID, false, false)
	}
	requireWaiting := func(err error) {
		t.Helper()
		retry, ok := hmessaging.AsRetryError(err)
		require.True(t, ok, "destroy must retry, got %v", err)
		require.Equal(t, destroyRetryDelay, retry.RetryDelay())
		require.ErrorIs(t, err, model.ErrEnvironmentBusy)
		require.Empty(t, core.recorded(), "a busy Environment never ends in delete_failed")
	}

	first := create(model.DeploymentModeDeploy)
	succeed(first.Id)
	running := create(model.DeploymentModeDeploy)
	queued := make([]uuid.UUID, 2)
	for index := range queued {
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		item, _, err := model.EnqueueDeployment(ctx, tx, model.NewDeploymentQueueItem{OrgID: "org", ProjectID: "project", EnvID: "environment", EnvironmentUUID: core.envUUID,
			Mode: model.DeploymentModeDeploy, Intent: json.RawMessage(`{}`), CreatedBy: uuid.New(), TTL: time.Hour}, 10)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		queued[index] = item.ID
	}

	require.NoError(t, destroy(), "destroy waits for the running Deployment's completion event")
	require.Empty(t, core.recorded())
	for _, id := range queued {
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		item, err := model.GetDeploymentQueueItem(ctx, tx, "org", id)
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		require.Equal(t, model.DeploymentQueueCancelled, item.State)
		require.Equal(t, opt.Of(model.CodeEnvironmentDeleting), item.ReasonCode)
	}

	// The running Deployment ends while another one starts: its lease must make destroy wait, not fail.
	succeed(running.Id)
	exec(`INSERT INTO environment_deployment_leases (environment_id, org_id, project_id, env_id, holder_id, purpose, deployment_id, expires_at)
		VALUES ($1, 'org', 'project', 'environment', $2, 'deployment', $2, now() + interval '1 hour')`, core.envUUID, first.Id)
	requireWaiting(destroy())
	exec(`DELETE FROM environment_deployment_leases WHERE environment_id=$1`, core.envUUID)

	// A plan run holds no lease but still makes destroy wait.
	plan := create(model.DeploymentModeDeployPlan)
	requireWaiting(destroy())
	succeed(plan.Id)

	require.NoError(t, destroy())
	changes := core.recorded()
	require.Len(t, changes, 1)
	require.Contains(t, changes[0], "Waiting for destroy deployment")
	last, _, _, _, err := store.GetLastDeployment(ctx, nil, "org", "project", "environment", model.GetLastDeploymentParams{StateChangeOnly: true})
	require.NoError(t, err)
	require.Equal(t, model.DeploymentModeDestroy, last.Mode)
}
