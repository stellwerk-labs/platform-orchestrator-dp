//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	pipelineapi "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The owned peer exercises normal typed HTTP/PG DP boundaries, not Core policy.
func countedRunnerPeer(t *testing.T, f *queueFixture, response func(int64) (cp.Runner, int)) *atomic.Int64 {
	t.Helper()
	calls := &atomic.Int64{}
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/orgs/"+queueOrg+"/runners/"+queueRunner {
			runner, status := response(calls.Add(1))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			assert.NoError(t, json.NewEncoder(w).Encode(runner))
			return
		}
		f.core.ServeHTTP(w, r)
	}))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.ControlPlaneClient = client
	return calls
}

func TestOrdinaryCompilationUsesTheOneResolvedRunnerResponse(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.RunnerRollouts = &RunnerRolloutService{OrgID: queueOrg}
	calls := countedRunnerPeer(t, f, func(call int64) (cp.Runner, int) {
		r := cp.Runner{OrgId: queueOrg, Id: queueRunner}
		namespace := "first"
		if call > 1 {
			namespace = "second"
		}
		assert.NoError(t, r.StateStorageConfiguration.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: namespace}))
		return r, http.StatusOK
	})
	dep := f.mustDeploy(queueBody(nil))
	require.EqualValues(t, 1, calls.Load())
	_, _, tofu, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, dep.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Regexp(t, `namespace\s*=\s*"first"`, string(tofu))
	require.NotContains(t, string(tofu), "second")
}

func TestFreshNativeRequestsNeverAcceptOrReserveThroughLegacyPublicPaths(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	runner := qualifiedNativeRunner(t)
	runner.OrgId = queueOrg
	runner.Id = queueRunner
	calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return runner, http.StatusOK })
	for _, fields := range []map[string]any{nil, {"plan_only": true}, {"is_dry_run": true}, {"mode": "rollback", "manifest": nil, "rollback_to_deployment_id": uuid.NewString()}} {
		response := f.deploy(queueBody(fields))
		require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), "not eligible for Native-first")
	}
	f.server.RunnerRollouts = &RunnerRolloutService{OrgID: queueOrg}
	response := f.deploy(queueBody(nil))
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.EqualValues(t, 5, calls.Load())
	f.core.mu.Lock()
	guardCount := len(f.core.guards)
	f.core.mu.Unlock()
	require.Zero(t, guardCount)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	for _, table := range []string{"deployments", "environment_deployment_queue", "environment_deployment_leases", "runner_job_reservations", "deployment_admission_receipts"} {
		var count int
		require.NoError(t, tx.QueryRowContext(f.ctx, "SELECT count(*) FROM "+table).Scan(&count))
		require.Zero(t, count, table)
	}
}

func TestAcceptedOrdinaryAndQueueReplayIgnoreCurrentRunnerAndFixtureGate(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	first := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "first")
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	var dep Deployment
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &dep))
	queued := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "queued")
	require.Equal(t, http.StatusAccepted, queued.Code, queued.Body.String())
	calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return cp.Runner{}, http.StatusServiceUnavailable })
	policies := &atomic.Int64{}
	offline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		policies.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(offline.Close)
	policyClient, err := pipelineapi.NewClientWithResponses(offline.URL)
	require.NoError(t, err)
	f.server.PipelineClient = policyClient
	f.server.RunnerRollouts = &RunnerRolloutService{OrgID: queueOrg}
	replay := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "first")
	require.Equal(t, http.StatusCreated, replay.Code, replay.Body.String())
	replay = f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "queued")
	require.Equal(t, http.StatusAccepted, replay.Code, replay.Body.String())
	require.Zero(t, calls.Load())
	require.Zero(t, policies.Load())
	f.server.RunnerRollouts = nil
	outside := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "first")
	require.Equal(t, http.StatusCreated, outside.Code, outside.Body.String())
	changed := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(map[string]any{"managed_logs": true}), f.actor, "queued")
	require.Equal(t, http.StatusConflict, changed.Code, changed.Body.String())
	require.Zero(t, calls.Load())
}

func TestQueuePromotionAssertsHistoricalNullAndCannotAdoptNativeAuthority(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	first := f.mustDeploy(queueBody(nil))
	queued := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "old-queue")
	require.Equal(t, http.StatusAccepted, queued.Code, queued.Body.String())
	var item DeploymentQueueItem
	require.NoError(t, json.Unmarshal(queued.Body.Bytes(), &item))
	runner := qualifiedNativeRunner(t)
	runner.OrgId = queueOrg
	runner.Id = queueRunner
	calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return runner, http.StatusOK })
	refused := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "native-busy")
	require.Equal(t, http.StatusConflict, refused.Code, refused.Body.String())
	require.EqualValues(t, 1, calls.Load())
	calls.Store(0)
	f.complete(first.Id, model.DeploymentStatusSucceeded)
	_, err := f.server.promoteDeploymentQueueHead(f.ctx, f.core.envUUID)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	stored, err := model.GetDeploymentQueueItem(f.ctx, tx, queueOrg, item.Id)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentQueueRejected, stored.State)
	require.Contains(t, stored.ReasonMessage.Or(""), "execution target changed")
	require.EqualValues(t, 1, calls.Load())
	var count int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployments`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestPreparedOrdinaryReceiptBlocksFreshMetadataAndReconstructsFrozenTarget(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	attempt := f.server.DeploymentAdmissions.NewAttempt(queueOrg, f.core.projectUUID, f.core.envUUID, f.actor, model.DeploymentModeDeploy, "pending")
	target := nativeTargetFixture()
	target.RunnerId = queueRunner
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	receipt := model.DeploymentAdmissionReceipt{
		NativeTarget:    target,
		OrgID:           queueOrg,
		AdmissionID:     attempt.AdmissionID,
		ProjectUUID:     f.core.projectUUID,
		EnvironmentUUID: f.core.envUUID,
		ActorID:         f.actor,
		Mode:            model.DeploymentModeDeploy,
		RequestDigest:   strings.Repeat("a", 64),
		Disposition:     "pending",
		ModuleVersions:  map[string]uuid.UUID{},
	}
	_, err = model.PrepareDeploymentAdmissionReceipt(f.ctx, tx, receipt)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return cp.Runner{}, http.StatusServiceUnavailable })
	policies := &atomic.Int64{}
	offline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		policies.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(offline.Close)
	policyClient, err := pipelineapi.NewClientWithResponses(offline.URL)
	require.NoError(t, err)
	f.server.PipelineClient = policyClient
	f.server.RunnerRollouts = &RunnerRolloutService{OrgID: queueOrg}
	response := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "pending")
	require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
	require.Zero(t, calls.Load())
	require.Zero(t, policies.Load())
	f.exec(`UPDATE deployment_admission_receipts SET created_at=now()-interval '2 minutes' WHERE admission_id=$1`, attempt.AdmissionID)
	require.NoError(t, f.server.DeploymentAdmissions.ReconcileUnsettled(f.ctx, 10))
	f.core.mu.Lock()
	guard := f.core.guards[attempt.AdmissionID]
	f.core.mu.Unlock()
	require.NotNil(t, guard)
	require.Equal(t, target, guard.request.NativeTarget)
}

func TestCommonCompilerRejectsSuppliedRunnerScopeAndNativeExpectationBeforeAdmission(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	environment := cp.Environment{Uuid: f.core.envUUID, RunnerId: func() *string { s := queueRunner; return &s }()}
	for _, test := range []struct {
		name     string
		runner   cp.Runner
		expected opt.Opt[*cp.NativeExecutionTargetReference]
	}{
		{name: "wrong scope", runner: cp.Runner{OrgId: "other", Id: queueRunner}},
		{name: "native expectation on legacy", runner: cp.Runner{OrgId: queueOrg, Id: queueRunner}, expected: opt.Of(nativeTargetFixture())},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			_, _, _, err = CreateDeploymentWithOptions(
				f.ctx,
				f.server.Logger,
				f.actor,
				queueOrg,
				queueProject,
				queueEnv,
				environment,
				model.DeploymentModeDeploy,
				opt.Empty[uuid.UUID](),
				DeploymentManifest{},
				nil,
				nil,
				nil,
				false,
				nil,
				nil,
				f.store,
				tx,
				DeploymentOptions{Runner: &test.runner, ExpectedNativeTarget: test.expected},
			)
			require.Error(t, err)
		})
	}
}

func TestHistoricalBuiltinReplayAcceptsOnlyItsTwoStoredIdentityForms(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit-v0", true: "explicit-v0"}[explicit], func(t *testing.T) {
			f := nativePlanFixture(t)
			var original uuid.UUID
			if !explicit {
				response := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "historical")
				require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
				var dep Deployment
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &dep))
				original = dep.Id
			} else {
				definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
				require.NoError(t, err)
				manifest := DeploymentManifest{Workloads: map[string]DeploymentManifestWorkload{}}
				environment := cp.Environment{Id: queueEnv, ProjectId: queueProject, Uuid: f.core.envUUID, ProjectUuid: &f.core.projectUUID, RunnerId: func() *string { s := queueRunner; return &s }()}
				key := "historical"
				created, err := f.server.createRequestedDeployment(f.ctx, f.server.Logger, f.actor, queueOrg, queueProject, queueEnv, environment, model.DeploymentModeDeploy, deploymentRequest{Pipeline: definition, Manifest: &manifest}, &key, false, opt.Empty[uuid.UUID]())
				require.NoError(t, err)
				original = created.deployment.Id
			}
			// Synthetic retained authority proves replay recovery, not Native public acceptance.
			target := nativeTargetFixture()
			target.RunnerId = queueRunner
			raw, err := json.Marshal(target)
			require.NoError(t, err)
			f.exec(`UPDATE deployments SET native_target=$2 WHERE id=$1`, original, raw)
			calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return cp.Runner{}, http.StatusServiceUnavailable })
			f.server.RunnerRollouts = nil
			replay := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "historical")
			require.Equal(t, http.StatusCreated, replay.Code, replay.Body.String())
			var same Deployment
			require.NoError(t, json.Unmarshal(replay.Body.Bytes(), &same))
			require.Equal(t, original, same.Id)
			require.Zero(t, calls.Load())
			for _, changed := range []map[string]any{
				{"managed_logs": true},
				{"module_versions": map[string]string{"postgres": "v2"}},
				{"pipeline": map[string]any{"id": "reviewed", "version": 1}},
				{"manifest": map[string]any{"workloads": map[string]any{"changed": map[string]any{"resources": map[string]any{}}}}},
			} {
				response := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(changed), f.actor, "historical")
				require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
			}
			require.Zero(t, calls.Load())
			f.iam.deny(f.actor, true)
			denied := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(nil), f.actor, "historical")
			require.Equal(t, http.StatusForbidden, denied.Code, denied.Body.String())
			require.Zero(t, calls.Load())
		})
	}
}
