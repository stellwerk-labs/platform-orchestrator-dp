//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func retainedNativeCustody(t *testing.T, f *queueFixture, id uuid.UUID) {
	t.Helper()
	target := nativeTargetFixture()
	target.RunnerId = queueRunner
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	f.exec(`UPDATE deployments SET native_target=$2 WHERE id=$1`, id, raw)
}

func TestStateCustodyRefusesOrdinaryRetargetingBeforeCatalogueAndCoreAdmission(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	base := f.mustDeploy(queueBody(nil))
	f.finish(base.Id, model.DeploymentStatusSucceeded)
	retainedNativeCustody(t, f, base.Id)
	var downstream atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "module-execution-catalogue") || strings.Contains(r.URL.Path, "deployment-admissions") {
			downstream.Add(1)
		}
		f.core.ServeHTTP(w, r)
	}))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.ControlPlaneClient = client
	f.server.DeploymentAdmissions.Core = client
	f.server.DeploymentAdmissions.ExecutionCatalogue = client
	before := f.count(`SELECT count(*) FROM deployments`)
	response := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "different-custody")
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "explicit handoff")
	require.Zero(t, downstream.Load())
	require.Equal(t, before, f.count(`SELECT count(*) FROM deployments`))
	require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_leases`))
	require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
}

func TestStateCustodyRetainedPlanRefusalPrecedesVaultMaterial(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	base := f.mustDeploy(queueBody(nil))
	f.finish(base.Id, model.DeploymentStatusSucceeded)
	retainedNativeCustody(t, f, base.Id)
	env, err := f.coreClient.GetEnvironmentWithResponse(f.ctx, queueOrg, queueProject, queueEnv)
	require.NoError(t, err)
	service := &RetainedPlanService{Server: f.server}
	f.server.Vault = nil
	before := f.count(`SELECT count(*) FROM deployments`)
	_, err = service.CreatePlan(f.ctx, f.actor, RetainedPlanRequest{
		OrgID: queueOrg, ProjectID: queueProject, EnvID: queueEnv, Environment: *env.JSON200,
		Manifest: DeploymentManifest{}, Backend: "kubernetes", Tool: "opentofu",
	}, uuid.New())
	require.ErrorContains(t, err, "explicit handoff", "custody refuses even while downstream Vault is unavailable")
	require.Equal(t, before, f.count(`SELECT count(*) FROM deployments`))
	require.Zero(t, f.count(`SELECT count(*) FROM retained_plans`))
	require.Zero(t, f.count(`SELECT count(*) FROM deployment_log_material`))
}

func TestStateCustodyRaceSealsThePreparedCoreGuardWithoutCreatingADeployment(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	base := f.mustDeploy(queueBody(nil))
	f.finish(base.Id, model.DeploymentStatusSucceeded)
	prepared, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.core.ServeHTTP(w, r)
		prefix := "/orgs/" + queueOrg + "/deployment-admissions/"
		if strings.HasPrefix(r.URL.Path, prefix) && !strings.Contains(strings.TrimPrefix(r.URL.Path, prefix), "/") {
			close(prepared)
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
	}))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.DeploymentAdmissions.Core = client
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "custody-race")
	}()
	select {
	case <-prepared:
	case <-time.After(5 * time.Second):
		t.Fatal("fresh request did not reach its prepared Core guard")
	}
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, model.LockEnvironmentMutation(f.ctx, tx, f.core.envUUID))
	target := nativeTargetFixture()
	target.RunnerId = queueRunner
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, base.Id, raw)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	unblock()
	response := <-done
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "explicit handoff")
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_admission_receipts WHERE disposition='absent'`))
	require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_leases`))
	require.Zero(t, f.count(`SELECT count(*) FROM deployment_pipeline_runs`))
	f.core.mu.Lock()
	defer f.core.mu.Unlock()
	require.Len(t, f.core.guards, 2)
	for _, guard := range f.core.guards {
		require.True(t, guard.settled, "the racing request must seal its own absent guard")
	}
}
