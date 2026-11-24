//go:build integration

package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	pipelineapi "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/plugincontext"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExternalPipelineMutationUsesOneRunnerResponse(t *testing.T) {
	testExternalDeploymentHTTPAdapterRollback(t, model.DeploymentStatusSucceeded, externalPipelineOneResponse)
}

func TestExternalTargetContinuityRefusesBeforeMutationAdmission(t *testing.T) {
	testExternalDeploymentHTTPAdapterRollback(t, model.DeploymentStatusSucceeded, externalTargetContinuity)
}

func verifyExternalTargetContinuity(t *testing.T, f externalRollbackFixture) {
	t.Helper()
	target := nativeTargetFixture()
	target.RunnerId = f.runnerID
	changed := *target
	changed.StateConfigurationDigest = strings.Repeat("c", 64)
	// These owned synthetic histories exercise refusal seams, not Native acceptance.
	for _, trial := range []struct {
		name, reason, path                  string
		first, base, later, retry, snapshot *cp.NativeExecutionTargetReference
	}{
		{name: "first forward changed", path: "rollback", first: target, reason: "first forward execution target"},
		{name: "base changed", path: "rollback", base: target, reason: "first forward execution target"},
		{name: "same site different state", path: "rollback", first: target, base: &changed, reason: "first forward execution target"},
		{name: "later forward changed", path: "later-forward", later: target, reason: "forward execution target differs"},
		{name: "legacy forward cannot adopt native lineage", first: target, base: target, path: "later-forward", reason: "forward execution target differs"},
		{name: "failed retry changed", path: "retry", retry: target, reason: "rollback retry execution target"},
		{name: "later plan cannot change original authority", first: target, base: target, path: "forward", reason: "planned execution target differs"},
		{name: "plan cannot adopt native base", base: target, path: "first-forward", reason: "planned execution target differs"},
		{name: "plan cannot replace legacy base", snapshot: target, path: "first-forward", reason: "planned execution target differs"},
	} {
		t.Run(trial.name, func(t *testing.T) {
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, tx.Rollback()) }()
			setTarget := func(id uuid.UUID, value *cp.NativeExecutionTargetReference) {
				var raw any
				if value != nil {
					encoded, err := json.Marshal(value)
					require.NoError(t, err)
					raw = encoded
				}
				_, err := tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, id, raw)
				require.NoError(t, err)
			}
			setTarget(f.deployment.Id, trial.first)
			setTarget(f.base.Id, trial.base)
			body := f.rollbackRequest
			switch trial.path {
			case "later-forward":
				id := uuid.New()
				_, err = tx.ExecContext(f.ctx, `INSERT INTO deployments(id,de_id,created_at,created_by,completed_at,mode,status,status_message,manifest,graph,tofu,runner_id,runner_log_level,module_artifacts)
    SELECT $2,de_id,created_at,created_by,completed_at,mode,status,status_message,manifest,graph,tofu,runner_id,runner_log_level,module_artifacts FROM deployments WHERE id=$1`, f.deployment.Id, id)
				require.NoError(t, err)
				receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(f.ctx, tx, f.orgID, f.pluginID, f.deployment.Id)
				require.NoError(t, err)
				receipt.RequestID = "target-later-" + id.String()
				receipt.DeploymentID = id
				_, err = model.CreateExternalOperationDeploymentReceipt(f.ctx, tx, *receipt)
				require.NoError(t, err)
				setTarget(id, trial.later)
				rollback := *body.Rollback
				rollback.ForwardDeploymentId = id
				body.Rollback = &rollback
			case "retry":
				_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET status='failed',completed_at=now() WHERE id=$1`, f.rollbackDeployment.Id)
				require.NoError(t, err)
				setTarget(f.rollbackDeployment.Id, trial.retry)
				rollback := *body.Rollback
				rollback.RetryOfRollbackDeploymentId = &f.rollbackDeployment.Id
				body.Rollback = &rollback
			case "forward", "first-forward":
				body = ExternalDeploymentAdmissionBody{}
				require.NoError(t, json.Unmarshal(f.bodyBytes, &body))
			}
			if trial.path == "first-forward" {
				_, err = tx.ExecContext(f.ctx, `DELETE FROM external_operation_rollback_anchors`)
				require.NoError(t, err)
				_, err = tx.ExecContext(f.ctx, `DELETE FROM external_operation_deployments WHERE deployment_id=$1`, f.deployment.Id)
				require.NoError(t, err)
			}
			if trial.snapshot != nil {
				snapshot, err := model.GetExternalDeploymentPlanSnapshotByID(f.ctx, tx, f.orgID, f.pluginID, body.Plan.SnapshotId)
				require.NoError(t, err)
				fingerprint := deploymentPlanFingerprint(DeploymentPlanCapture{NativeTarget: trial.snapshot, BaseDeploymentID: snapshot.BaseDeploymentID, Manifest: snapshot.Manifest, Graph: snapshot.Graph, Diff: snapshot.Diff, ModuleArtifacts: snapshot.ModuleArtifacts})
				raw, err := json.Marshal(trial.snapshot)
				require.NoError(t, err)
				_, err = tx.ExecContext(f.ctx, `UPDATE external_deployment_plan_snapshots SET native_target=$2,fingerprint=$3 WHERE id=$1`, snapshot.ID, raw, fingerprint)
				require.NoError(t, err)
				plan := *body.Plan
				plan.Fingerprint = fingerprint
				body.Plan = &plan
			}
			counts := func() []int {
				var values []int
				for _, query := range []string{
					`SELECT count(*) FROM deployments`, `SELECT count(*) FROM deployment_admission_receipts`, `SELECT count(*) FROM environment_deployment_leases`, `SELECT count(*) FROM runner_job_reservations`, `SELECT count(*) FROM pending_event_messages`, `SELECT count(*) FROM deployment_pipeline_runs WHERE command IS NOT NULL`, `SELECT count(*) FROM deployment_dispatch_budgets WHERE observation_command IS NOT NULL`,
				} {
					var value int
					require.NoError(t, tx.QueryRowContext(f.ctx, query).Scan(&value))
					values = append(values, value)
				}
				return values
			}
			before := counts()
			ordinary := f.ordinaryCalls.Load()
			published := len(f.publisher.Messages())
			attempt := f.adapter.OrdinaryAdmissions.NewAttempt(f.orgID, f.projectUUID, f.environmentUUID, f.actorID, model.DeploymentModeDeploy, "lineage-check-"+uuid.NewString())
			attempt.ExternalDispatch = &model.ExternalAdmissionDispatch{PluginID: f.pluginID, OperationID: f.operationID, DispatchID: f.claims.RequestID, Fence: body.Fence, RequestDigest: f.claims.BodySHA256}
			environment := cp.Environment{Id: f.envID, ProjectId: f.projectID, Uuid: f.environmentUUID, ProjectUuid: &f.projectUUID, RunnerId: ref.Ref(f.runnerID)}
			switch trial.path {
			case "forward", "first-forward":
				_, err = f.adapter.createExternalDeploymentFromSnapshot(f.ctx, tx, f.actorID, f.orgID, f.pluginID, body, environment, f.material.Recipient, externalPipelineAdmission{Attempt: attempt})
			default:
				_, err = f.adapter.createExternalRollback(f.ctx, tx, f.actorID, f.orgID, f.pluginID, body, environment, f.material.Recipient, externalPipelineAdmission{Attempt: attempt})
			}
			require.ErrorContains(t, err, trial.reason)
			require.Equal(t, before, counts(), "lineage refusal must precede admission and execution writes")
			require.Equal(t, ordinary, f.ordinaryCalls.Load())
			require.Len(t, f.publisher.Messages(), published)
		})
	}
	for _, missing := range []string{"metadata", "fresh Server"} {
		t.Run(missing, func(t *testing.T) {
			realServer := f.adapter.Server
			if missing == "metadata" {
				f.metadataUnavailable.Store(true)
			} else {
				f.adapter.Server = nil
			}
			defer func() { f.metadataUnavailable.Store(false); f.adapter.Server = realServer }()
			claims := f.claims
			claims.RequestID = "unavailable-" + uuid.NewString()
			assertion, err := plugincontext.Sign(f.privateKey, claims, f.now)
			require.NoError(t, err)
			grants, ordinary := f.checks.Load(), f.ordinaryCalls.Load()
			response := f.do(f.bodyBytes, assertion, claims.RequestID)
			require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
			require.Equal(t, grants+1, f.checks.Load(), "current external grant reauthorization stays mandatory")
			require.Equal(t, ordinary, f.ordinaryCalls.Load())
		})
	}
}

func TestExternalResolverUsesDirectionalDefaultsAndRefusesNativeFallback(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	forward, rollback := publishedRecipeFixture(t), publishedRecipeFixture(t)
	choices := pipelineapi.PipelineChoices{
		Policy:                   pipelineapi.PipelinePolicy{Enabled: true, DefaultVersionId: &forward.Id},
		Versions:                 []pipelineapi.PublishedPipelineVersion{forward, rollback},
		EffectiveDefault:         &pipelineapi.PipelineReference{VersionId: &forward.Id, Digest: forward.Digest},
		EffectiveRollbackDefault: &pipelineapi.PipelineReference{VersionId: &rollback.Id, Digest: rollback.Digest},
	}
	var policyMu, runnerMu sync.Mutex
	policyPeer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		policyMu.Lock()
		defer policyMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		assert.NoError(t, json.NewEncoder(w).Encode(choices))
	}))
	t.Cleanup(policyPeer.Close)
	var err error
	f.server.PipelineClient, err = pipelineapi.NewClientWithResponses(policyPeer.URL)
	require.NoError(t, err)
	runner := cp.Runner{Id: queueRunner, OrgId: queueOrg}
	native := qualifiedNativeRunner(t)
	native.OrgId = queueOrg
	native.Id = queueRunner
	agent, err := native.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
	require.NoError(t, err)
	raw, err := json.Marshal(
		map[string]any{
			"type":                "kubernetes-agent",
			"key":                 agent.Key,
			"supported_protocols": []string{model.PipelineProtocol},
			"capabilities":        []string{"pipeline.recipe.v1", "local.exec", "artifacts.read", "database.local"},
			"max_parallel_jobs":   2,
			"job":                 map[string]any{"namespace": "runner", "service_account": "runner"},
		},
	)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &runner.RunnerConfiguration))
	require.NoError(t, runner.StateStorageConfiguration.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: "state"}))
	reads := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { runnerMu.Lock(); defer runnerMu.Unlock(); return runner, http.StatusOK })
	adapter := &ExternalDeploymentHTTPAdapter{Server: f.server, OrdinaryAdmissions: f.server.DeploymentAdmissions}
	environment := cp.Environment{Id: queueEnv, ProjectId: queueProject, Uuid: f.core.envUUID, ProjectUuid: &f.core.projectUUID, RunnerId: ref.Ref(queueRunner)}
	for _, trial := range []struct {
		mode model.DeploymentMode
		want uuid.UUID
	}{{model.DeploymentModeDeploy, forward.Id}, {model.DeploymentModeRollback, rollback.Id}} {
		before := reads.Load()
		selected, err := adapter.resolvePipelineAdmission(f.ctx, f.actor, queueOrg, "dev.example.rollouts", uuid.NewString(), &environment, trial.mode, nil)
		require.NoError(t, err)
		require.Equal(t, trial.want.String(), selected.Definition.ID)
		require.NotNil(t, selected.Runner)
		require.Equal(t, before+1, reads.Load())
	}
	policyMu.Lock()
	choices.EffectiveRollbackDefault = nil
	policyMu.Unlock()
	before := reads.Load()
	_, err = adapter.resolvePipelineAdmission(f.ctx, f.actor, queueOrg, "dev.example.rollouts", uuid.NewString(), &environment, model.DeploymentModeRollback, nil)
	require.ErrorContains(t, err, "Rollback requires")
	require.Equal(t, before, reads.Load())
	selectedRef := PipelineReference{VersionId: &forward.Id, Digest: forward.Digest}
	selected, err := adapter.resolvePipelineAdmission(f.ctx, f.actor, queueOrg, "dev.example.rollouts", uuid.NewString(), &environment, model.DeploymentModeDeploy, &selectedRef)
	require.NoError(t, err)
	require.Equal(t, forward.Digest, selected.Definition.Digest)
	selectedRef.Digest = strings.Repeat("0", 64)
	before = reads.Load()
	_, err = adapter.resolvePipelineAdmission(f.ctx, f.actor, queueOrg, "dev.example.rollouts", uuid.NewString(), &environment, model.DeploymentModeDeploy, &selectedRef)
	require.Error(t, err)
	require.Equal(t, before, reads.Load())
	f.server.PipelineClient = nil
	_, err = adapter.resolvePipelineAdmission(f.ctx, f.actor, queueOrg, "dev.example.rollouts", uuid.NewString(), &environment, model.DeploymentModeDeploy, &selectedRef)
	require.ErrorContains(t, err, "policy service")
	runnerMu.Lock()
	runner = native
	runnerMu.Unlock()
	_, err = adapter.resolvePipelineAdmission(f.ctx, f.actor, queueOrg, "dev.example.rollouts", uuid.NewString(), &environment, model.DeploymentModeDeploy, nil)
	require.ErrorContains(t, err, "not eligible for Native-first")
	f.core.mu.Lock()
	guards := len(f.core.guards)
	f.core.mu.Unlock()
	require.Zero(t, guards)
}

func verifyExternalPlanCaptureReplay(t *testing.T, f externalRollbackFixture) {
	t.Helper()
	require.EqualValues(t, 1, f.runnerReads.Load(), "capture uses exactly its existing assigned Runner response")
	f.metadataUnavailable.Store(true)
	defer f.metadataUnavailable.Store(false)
	assertion, err := plugincontext.Sign(f.privateKey, f.planClaims, f.now)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, f.planPath, bytes.NewReader(f.planBodyBytes))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", f.planClaims.RequestID)
	request.Header.Set(plugincontext.Header, assertion)
	response := httptest.NewRecorder()
	f.server.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	original, err := json.Marshal(f.plan)
	require.NoError(t, err)
	require.JSONEq(t, string(original), response.Body.String())
	require.Zero(t, f.metadataReads.Load())
}

func verifyExternalAcceptedMetadataOutageReplay(t *testing.T, f externalRollbackFixture) {
	t.Helper()
	f.metadataUnavailable.Store(true)
	server := f.adapter.Server
	f.adapter.Server = nil
	defer func() { f.adapter.Server = server; f.metadataUnavailable.Store(false) }()
	ordinary := f.ordinaryCalls.Load()
	response := f.do(f.bodyBytes, f.assertion, f.claims.RequestID)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.EqualValues(t, 3, f.checks.Load(), "replay must reauthorize before loading its receipt")
	require.Zero(t, f.metadataReads.Load())
	require.Equal(t, ordinary, f.ordinaryCalls.Load())
}

func verifyExternalMutationRunnerResponse(t *testing.T, f externalRollbackFixture, scenario externalRollbackScenario) {
	t.Helper()
	require.EqualValues(t, 1, f.runnerReads.Load(), "mutation reuses the fetched Runner during compilation")
	if scenario != externalPipelineOneResponse {
		return
	}
	_, _, tofu, _, err := f.store.GetDeployment(f.ctx, nil, f.orgID, f.deployment.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Regexp(t, `namespace\s*=\s*"state"`, string(tofu))
	require.NotContains(t, string(tofu), "wrong-second-response")
}

func prepareExternalPipelineScenario(t *testing.T, server *Server, url string, scenario externalRollbackScenario) *PipelineReference {
	t.Helper()
	if scenario != externalPipelineOneResponse {
		return nil
	}
	var err error
	server.PipelineClient, err = pipelineapi.NewClientWithResponses(url)
	require.NoError(t, err)
	builtin := pipelineapi.BuiltinReference()
	selected := PipelineReferenceBuiltin(*builtin.Builtin)
	return &PipelineReference{Builtin: &selected, Digest: builtin.Digest}
}

func externalRollbackRunnerResponse(now time.Time, scenario externalRollbackScenario, count int64) map[string]any {
	namespace := "state"
	config := map[string]any{"type": "kubernetes", "cluster": map[string]any{"auth": map[string]any{}, "cluster_data": map[string]any{"certificate_authority_data": "", "server": ""}}, "job": map[string]any{"namespace": "runner", "service_account": "runner"}}
	if scenario == externalPipelineOneResponse {
		if count > 1 {
			namespace = "wrong-second-response"
		}
		config = map[string]any{
			"type":                "kubernetes-agent",
			"key":                 "-----BEGIN PUBLIC KEY-----\nMCowBQYDK2VwAyEAc5dgCx4ano39JT0XgTsHnts3jej+5xl7ZAwSIrKpef0=\n-----END PUBLIC KEY-----",
			"supported_protocols": []string{model.PipelineProtocol},
			"job":                 map[string]any{"namespace": "runner", "service_account": "runner"},
		}
	}
	return map[string]any{"created_at": now, "updated_at": now, "id": "runner-1", "org_id": "acme", "runner_configuration": config, "state_storage_configuration": map[string]any{"type": "kubernetes", "namespace": namespace}}
}
