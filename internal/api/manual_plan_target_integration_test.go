//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func manualNativeTarget(t *testing.T) *cp.NativeExecutionTargetReference {
	t.Helper()
	runner := qualifiedNativeRunner(t)
	runner.OrgId, runner.Id = queueOrg, queueRunner
	target, err := nativeExecutionTarget(queueOrg, queueRunner, runner, model.DeploymentModeDeploy, nil)
	require.NoError(t, err)
	return target
}

// These synthetic retained records test refusal, not accepted Native execution.
func setManualPlanTargets(t *testing.T, f *queueFixture, id uuid.UUID, deployment, capture *cp.NativeExecutionTargetReference) {
	t.Helper()
	var stored any
	if deployment != nil {
		raw, err := json.Marshal(deployment)
		require.NoError(t, err)
		stored = raw
	}
	f.exec(`UPDATE deployments SET native_target=$2 WHERE id=$1`, id, stored)
	raw, err := json.Marshal(capture)
	require.NoError(t, err)
	f.exec(`UPDATE deployment_pipeline_runs SET plan_capture=jsonb_set(plan_capture,'{native_target}',$2::jsonb) WHERE deployment_id=$1`, id, raw)
}

func TestManualPlanRetainedTargetIntegrityAndNativeDecoderRefusal(t *testing.T) {
	target := manualNativeTarget(t)
	other := *target
	other.RunnerUuid = uuid.New()
	for _, test := range []struct {
		name                string
		deployment, capture *cp.NativeExecutionTargetReference
		message             string
	}{
		{"source only", target, nil, "captured execution target is inconsistent"},
		{"capture only", nil, target, "captured execution target is inconsistent"},
		{"different registrations", target, &other, "captured execution target is inconsistent"},
		{"invalid Native retained command", target, target, "Saved-plan command is unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := nativePlanFixture(t)
			plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
			completeNativePlan(t, f, plan.Id)
			setManualPlanTargets(t, f, plan.Id, test.deployment, test.capture)
			f.exec(`UPDATE deployment_pipeline_runs SET command='{}' WHERE deployment_id=$1`, plan.Id)
			calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return cp.Runner{}, http.StatusServiceUnavailable })
			admissions, outbox := f.core.admissions(), f.count(`SELECT count(*) FROM pending_event_messages`)
			_, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "refuse")
			require.ErrorContains(t, err, test.message)
			require.Zero(t, calls.Load())
			require.Equal(t, admissions, f.core.admissions())
			require.Equal(t, outbox, f.count(`SELECT count(*) FROM pending_event_messages`))
			require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
			require.Zero(t, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, plan.Id))
			require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations`))
			require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_leases`))
		})
	}
}

func TestManualSavedApplyAssertsHistoricalNullAgainstCurrentNative(t *testing.T) {
	for _, test := range []struct{ org, runner, message string }{
		{queueOrg, queueRunner, "saved-plan execution target changed"},
		{"foreign-org", queueRunner, "saved-plan Runner identity changed"},
		{queueOrg, "foreign-runner", "saved-plan Runner identity changed"},
	} {
		t.Run(test.org+"/"+test.runner, func(t *testing.T) {
			f := nativePlanFixture(t)
			plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
			completeNativePlan(t, f, plan.Id)
			runner := qualifiedNativeRunner(t)
			runner.OrgId, runner.Id = test.org, test.runner
			calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return runner, http.StatusOK })
			admissions := f.core.admissions()
			_, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "old-plan")
			require.ErrorContains(t, err, test.message)
			require.EqualValues(t, 1, calls.Load())
			require.Equal(t, admissions, f.core.admissions())
			require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
			require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations`))
			require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_leases`))
		})
	}
}

func TestManualApplyReplayChecksRetainedIntegrityWithoutMutableLookup(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	child, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "accepted")
	require.NoError(t, err)
	calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return cp.Runner{}, http.StatusServiceUnavailable })
	again, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "accepted")
	require.NoError(t, err)
	require.Equal(t, child.Id, again.Id)
	setManualPlanTargets(t, f, plan.Id, nil, manualNativeTarget(t))
	_, err = f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "accepted")
	require.ErrorContains(t, err, "captured execution target is inconsistent")
	require.Zero(t, calls.Load())
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, plan.Id))
}

func TestManualReplanCompilesOneFreshRunnerResponseAndRecoversOffline(t *testing.T) {
	f := nativePlanFixture(t)
	f.server.RunnerRollouts = &RunnerRolloutService{OrgID: queueOrg}
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true, "pipeline": map[string]any{"id": "default", "version": 1}}))
	completeNativePlan(t, f, plan.Id)
	var offline atomic.Bool
	var policies atomic.Int64
	policyPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		policies.Add(1)
		if offline.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(p.PipelineChoices{}))
	}))
	t.Cleanup(policyPeer.Close)
	client, err := p.NewClientWithResponses(policyPeer.URL)
	require.NoError(t, err)
	f.server.PipelineClient = client
	calls := countedRunnerPeer(t, f, func(n int64) (cp.Runner, int) {
		if offline.Load() {
			return cp.Runner{}, http.StatusServiceUnavailable
		}
		runner := cp.Runner{OrgId: queueOrg, Id: queueRunner}
		assert.NoError(t, json.Unmarshal([]byte(`{"type":"kubernetes-agent","supported_protocols":["stellwerk.pipeline/v1","stellwerk.pipeline.plan-apply/v1"]}`), &runner.RunnerConfiguration))
		namespace := "fresh-first"
		if n > 1 {
			namespace = "changed-second"
		}
		assert.NoError(t, runner.StateStorageConfiguration.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: namespace}))
		return runner, http.StatusOK
	})
	replanned, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "fresh")
	require.NoError(t, err)
	require.EqualValues(t, 1, calls.Load())
	require.EqualValues(t, 1, policies.Load())
	_, _, tofu, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, replanned.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Regexp(t, `namespace\s*=\s*"fresh-first"`, string(tofu))
	run, err := f.server.readDeploymentPipeline(f.ctx, replanned.Id)
	require.NoError(t, err)
	require.Equal(t, &model.PipelineSelection{ID: "default", Version: 1}, run.Definition.RequestedSelection)
	offline.Store(true)
	again, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "fresh")
	require.NoError(t, err)
	require.Equal(t, replanned.Id, again.Id)
	require.EqualValues(t, 1, calls.Load())
	require.EqualValues(t, 1, policies.Load())
}

func TestManualReplanValidatesFreshEnvironmentWithoutOldRunnerConstraint(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	var calls atomic.Int64
	var invalid atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/orgs/" + queueOrg + "/projects/" + queueProject + "/envs/" + queueEnv:
			runner := "replacement"
			env := cp.Environment{Id: queueEnv, ProjectId: queueProject, ProjectUuid: &f.core.projectUUID, Uuid: f.core.envUUID, RunnerId: &runner, Status: cp.EnvironmentStatusActive}
			switch invalid.Load() {
			case 1:
				env.Uuid = uuid.New()
			case 2:
				env.ProjectId = "foreign-project"
			case 3:
				env.Id = "foreign-environment"
			case 4:
				env.Status = cp.EnvironmentStatusDeleting
			case 5:
				env.ProjectUuid = nil
			}
			assert.NoError(t, json.NewEncoder(w).Encode(env))
		case "/orgs/" + queueOrg + "/runners/replacement":
			calls.Add(1)
			runner := cp.Runner{OrgId: queueOrg, Id: "replacement"}
			assert.NoError(t, json.Unmarshal([]byte(`{"type":"kubernetes-agent","supported_protocols":["stellwerk.pipeline/v1","stellwerk.pipeline.plan-apply/v1"]}`), &runner.RunnerConfiguration))
			assert.NoError(t, runner.StateStorageConfiguration.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: "fresh"}))
			assert.NoError(t, json.NewEncoder(w).Encode(runner))
		default:
			f.core.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.ControlPlaneClient = client
	for kind := int64(1); kind <= 5; kind++ {
		invalid.Store(kind)
		_, err = f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "wrong-identity")
		require.ErrorContains(t, err, "Environment identity or lifecycle changed")
		require.Zero(t, calls.Load())
		require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
	}
	invalid.Store(0)
	replanned, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "fresh-runner")
	require.NoError(t, err)
	require.Equal(t, "replacement", replanned.RunnerId)
	require.EqualValues(t, 1, calls.Load())
	require.Nil(t, replanned.NativeTarget)
}

func TestManualReplanRefusesIneligibleNativeBeforeCreatingWork(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	runner := qualifiedNativeRunner(t)
	runner.OrgId, runner.Id = queueOrg, queueRunner
	calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return runner, http.StatusOK })
	admissions := f.core.admissions()
	_, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "native-refused")
	require.ErrorContains(t, err, "not eligible for Native-first")
	require.EqualValues(t, 1, calls.Load())
	require.Equal(t, admissions, f.core.admissions())
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
	require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations`))
}
