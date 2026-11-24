//go:build integration

package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/labstack/echo/v4"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	p "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/pipelineapi"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// These peers exercise signed HTTP and real PG, not actual Core/IAM policy.
func wireRemediationTargetAdapter(t *testing.T, f *observationRecoveryFixture, client cp.ClientWithResponsesInterface) *ExternalDeploymentHTTPAdapter {
	t.Helper()
	f.server.ControlPlaneClient = client
	adapter := &ExternalDeploymentHTTPAdapter{Server: f.server, Issuer: "core", VerificationKey: f.private.Public().(ed25519.PublicKey),
		Admission: ExternalDeploymentAdmissionService{Database: f.store}, OrdinaryAdmissions: f.server.DeploymentAdmissions,
		ControlPlaneClient: client, EventPublisher: f.server.EventPublisher}
	f.router = echo.New()
	f.router.POST(ObservationRedeployPath, adapter.HandleObservationRedeploy)
	f.router.POST(ObservationRedeployResolvePath, adapter.HandleObservationRedeployResolve)
	return adapter
}

func remediationOwnershipCounts(t *testing.T, f observationRecoveryFixture) []int {
	t.Helper()
	var counts []int
	for _, table := range []string{"deployments", "deployment_admission_receipts", "external_operation_material", "environment_observation_redeploy_receipts", "environment_deployment_leases", "runner_job_reservations", "pending_event_messages"} {
		counts = append(counts, f.count("SELECT count(*) FROM "+table))
	}
	return counts
}

func TestObservationRemediationNativeCurrentRefusesBeforeOrdinaryGuard(t *testing.T) {
	f := newObservationRecoveryFixture(t)
	current := qualifiedNativeRunner(t)
	current.OrgId, current.Id = queueOrg, queueRunner
	var runnerReads, guardCalls atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/orgs/"+queueOrg+"/runners/"+queueRunner {
			runnerReads.Add(1)
			w.Header().Set("Content-Type", "application/json")
			assert.NoError(t, json.NewEncoder(w).Encode(current))
			return
		}
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/orgs/"+queueOrg+"/deployment-admissions/") && !strings.HasSuffix(r.URL.Path, "/seal-absence") {
			guardCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error":"native_target_required","message":"ordinary guard refused missing Native authority"}`))
			return
		}
		f.core.ServeHTTP(w, r)
	}))
	t.Cleanup(peer.Close)
	client, err := cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	f.server.DeploymentAdmissions, err = NewOrdinaryDeploymentAdmissionService(f.store, peer.URL, f.tokenFile, http.DefaultClient)
	require.NoError(t, err)
	wireRemediationTargetAdapter(t, &f, client)
	before := remediationOwnershipCounts(t, f)
	response := f.forward(t, f.body)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Zero(t, guardCalls.Load(), "DP must refuse before asking Core to authorize missing Native authority")
	require.EqualValues(t, 1, runnerReads.Load())
	require.Equal(t, before, remediationOwnershipCounts(t, f))
}

func TestObservationRemediationNativeBaselineCannotFallBackToLegacy(t *testing.T) {
	f := newObservationRecoveryFixture(t)
	base, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, f.body.BaselineId, model.GetModeDefault)
	require.NoError(t, err)
	target := nativeTargetFixture()
	target.RunnerId = queueRunner
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	fingerprint := model.ObservationBaselineFingerprint(manifest, tofu, graph, base.ModuleArtifacts, target)
	// Retained synthetic Native history tests refusal, not Native execution.
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$3 WHERE id IN ($1,$2)`, base.Id, f.runID, raw)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE environment_observation_receipts SET receipt=jsonb_set(receipt,'{baseline_fingerprint}',to_jsonb($2::text)) WHERE deployment_id=$1`, f.runID, fingerprint)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	f.body.BaselineFingerprint = fingerprint
	reads := countedRunnerPeer(t, f.queueFixture, func(int64) (cp.Runner, int) { return cp.Runner{Id: queueRunner, OrgId: queueOrg}, http.StatusOK })
	wireRemediationTargetAdapter(t, &f, f.server.ControlPlaneClient)
	before, guards := remediationOwnershipCounts(t, f), f.core.admissions()
	response := f.forward(t, f.body)
	require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "remediation execution target differs")
	require.EqualValues(t, 1, reads.Load())
	require.Equal(t, guards, f.core.admissions())
	require.Equal(t, before, remediationOwnershipCounts(t, f))
}

func TestObservationRemediationFreshScopeAndServerAreRequired(t *testing.T) {
	for _, trial := range []struct {
		name, project, environment string
		status                     int
	}{{"project slug", "foreign", queueEnv, http.StatusConflict}, {"environment slug", queueProject, "foreign", http.StatusConflict}, {"missing Server", queueProject, queueEnv, http.StatusServiceUnavailable}} {
		t.Run(trial.name, func(t *testing.T) {
			f := newObservationRecoveryFixture(t)
			var metadataReads atomic.Int64
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				metadataReads.Add(1)
				if r.Method == http.MethodGet && r.URL.Path == "/orgs/"+queueOrg+"/projects/"+queueProject+"/envs/"+queueEnv {
					runner := queueRunner
					env := cp.Environment{Id: trial.environment, ProjectId: trial.project, ProjectUuid: &f.core.projectUUID, Uuid: f.core.envUUID, RunnerId: &runner, Status: cp.EnvironmentStatusActive}
					w.Header().Set("Content-Type", "application/json")
					assert.NoError(t, json.NewEncoder(w).Encode(env))
					return
				}
				f.core.ServeHTTP(w, r)
			}))
			t.Cleanup(peer.Close)
			client, err := cp.NewClientWithResponses(peer.URL)
			require.NoError(t, err)
			adapter := wireRemediationTargetAdapter(t, &f, client)
			if trial.name == "missing Server" {
				adapter.Server = nil
			}
			before, guards := remediationOwnershipCounts(t, f), f.core.admissions()
			response := f.forward(t, f.body)
			require.Equal(t, trial.status, response.Code, response.Body.String())
			require.EqualValues(t, 1, metadataReads.Load(), "refusal must precede Runner lookup")
			require.Equal(t, guards, f.core.admissions())
			require.Equal(t, before, remediationOwnershipCounts(t, f))
		})
	}
}

func TestObservationRemediationUsesOneRunnerForDefaultAndExplicitPipeline(t *testing.T) {
	for _, trial := range []struct {
		name     string
		explicit bool
	}{{"project default", false}, {"explicit", true}} {
		t.Run(trial.name, func(t *testing.T) {
			f := newObservationRecoveryFixture(t)
			version := publishedRecipeFixture(t)
			choices := p.PipelineChoices{Policy: p.PipelinePolicy{Enabled: true, DefaultVersionId: &version.Id}, Versions: []p.PublishedPipelineVersion{version}, EffectiveDefault: &p.PipelineReference{VersionId: &version.Id, Digest: version.Digest}}
			policy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				assert.NoError(t, json.NewEncoder(w).Encode(choices))
			}))
			t.Cleanup(policy.Close)
			var err error
			f.server.PipelineClient, err = p.NewClientWithResponses(policy.URL)
			require.NoError(t, err)
			reads := countedRunnerPeer(t, f.queueFixture, func(call int64) (cp.Runner, int) {
				runner := cp.Runner{Id: queueRunner, OrgId: queueOrg}
				assert.NoError(t, json.Unmarshal([]byte(`{"type":"kubernetes-agent","supported_protocols":["stellwerk.pipeline/v1"],"capabilities":["pipeline.recipe.v1","local.exec","artifacts.read","database.local"]}`), &runner.RunnerConfiguration))
				assert.NoError(t, runner.StateStorageConfiguration.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: "first"}))
				if call > 1 {
					assert.NoError(t, runner.StateStorageConfiguration.FromS3StorageConfiguration(cp.S3StorageConfiguration{Bucket: "second"}))
				}
				return runner, http.StatusOK
			})
			adapter := wireRemediationTargetAdapter(t, &f, f.server.ControlPlaneClient)
			adapter.Materials = ExternalOperationMaterialService{Vault: managedLogVault(t)}
			if trial.explicit {
				f.body.Pipeline = &observation.PipelineReference{VersionId: &version.Id, Digest: version.Digest}
			}
			base, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, f.body.BaselineId, model.GetModeDefault)
			require.NoError(t, err)
			response := f.forward(t, f.body)
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			require.EqualValues(t, 1, reads.Load(), "backend and pipeline must use the first retained Runner response")
			var receipt observation.ObservationRedeployReceipt
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
			child, actualManifest, actualTofu, actualGraph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, receipt.DeploymentId, model.GetModeDefault)
			require.NoError(t, err)
			require.Equal(t, manifest, actualManifest)
			require.Equal(t, tofu, actualTofu)
			require.Equal(t, graph, actualGraph)
			require.Equal(t, base.ModuleArtifacts, child.ModuleArtifacts)
			require.Nil(t, child.NativeTarget)
			readTx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			var backend string
			require.NoError(t, readTx.QueryRowContext(f.ctx, `SELECT backend FROM deployment_pipeline_runs WHERE deployment_id=$1`, child.Id).Scan(&backend))
			require.Equal(t, "kubernetes", backend)
			ordinary, err := model.GetDeploymentAdmissionReceiptByDeployment(f.ctx, readTx, child.Id)
			require.NoError(t, err)
			require.Nil(t, ordinary.NativeTarget)
			require.Equal(t, version.Digest, ordinary.Pipeline.Digest)
			material, err := model.GetExternalOperationMaterial(f.ctx, readTx, queueOrg, "dev.example.plugin", f.body.RequestId)
			require.NoError(t, err)
			require.Equal(t, material.Recipient, child.EncryptedOutputsRecipient.Or(""))
			require.NotEmpty(t, child.EncryptedLogsRecipient.Or(""))
			logs, err := model.GetDeploymentLogMaterial(f.ctx, readTx, queueOrg, child.Id)
			require.NoError(t, err)
			require.Equal(t, logs.Recipient, child.EncryptedLogsRecipient.Must())
			require.NotEqual(t, material.Recipient, logs.Recipient)
			require.Contains(t, logs.EncryptedIdentity, "vault:v")
			require.NotContains(t, logs.EncryptedIdentity, "AGE-SECRET-KEY")
			require.NoError(t, readTx.Rollback())
			guards := f.core.admissions()
			var unavailableReads atomic.Int64
			unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				unavailableReads.Add(1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			t.Cleanup(unavailable.Close)
			adapter.ControlPlaneClient, err = cp.NewClientWithResponses(unavailable.URL)
			require.NoError(t, err)
			adapter.Server = nil
			unavailableConfig := vaultapi.DefaultConfig()
			unavailableConfig.Address = unavailable.URL
			unavailableVault, err := vaultapi.NewClient(unavailableConfig)
			require.NoError(t, err)
			adapter.Materials.Vault = vault.NewVaultClient(unavailableVault, zap.NewNop())
			replay := f.forward(t, f.body)
			require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
			require.Equal(t, response.Body.String(), replay.Body.String())
			require.EqualValues(t, 1, reads.Load())
			require.Equal(t, guards, f.core.admissions())
			require.Zero(t, unavailableReads.Load())
			readTx, err = f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			sameLogs, err := model.GetDeploymentLogMaterial(f.ctx, readTx, queueOrg, child.Id)
			require.NoError(t, err)
			require.Equal(t, logs, sameLogs)
			require.NoError(t, readTx.Rollback())
			require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_log_material`))
			changed := f.body
			changed.BaselineId = uuid.New()
			require.Equal(t, http.StatusConflict, f.forward(t, changed).Code)
			path := "/orgs/" + queueOrg + "/plugins/dev.example.plugin/observations/v1/runs/" + f.runID.String() + "/redeploy"
			require.Equal(t, http.StatusConflict, f.serve(f.request(t, path, ObservationRedeployAction, f.body, uuid.New())).Code)
		})
	}
}

// A real Native ordinary baseline and synthetic accepted observation state qualify
// only remediation. Public Native observation admission remains unsupported.
func TestNativeProducerObservationRemediationUsesOrdinaryAdmission(t *testing.T) {
	f := observationRecoveryFixture{queueFixture: newNativeProducerFixture(t)}
	ordinary := f.mustDeploy(queueBody(nil))
	baselineTx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = baselineTx.Rollback() }()
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, baselineTx, ordinary.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	require.NoError(t, model.SettleDeploymentPipeline(f.ctx, baselineTx, ordinary.Id, model.DeploymentStatusSucceeded))
	require.NoError(t, baselineTx.Commit())
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, ordinary.Id))
	baselineReceipt := nativeProducerReceipt(t, f.queueFixture, ordinary.Id)
	require.NotNil(t, baselineReceipt.NativeTarget)
	require.Equal(t, "committed", baselineReceipt.Disposition)
	require.True(t, baselineReceipt.CoreSettledAt.Valid)
	f.core.mu.Lock()
	guard := f.core.guards[baselineReceipt.AdmissionID]
	f.core.mu.Unlock()
	require.NotNil(t, guard)
	require.Equal(t, baselineReceipt.RequestDigest, guard.request.RequestDigest)
	require.Equal(t, baselineReceipt.NativeTarget, guard.request.NativeTarget)
	require.Equal(t, baselineReceipt.NativeAdmissionWatermark, guard.request.NativeAdmissionWatermark)
	require.Equal(t, baselineReceipt.CoreEpoch, guard.epoch)
	require.True(t, guard.settled)
	base, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, queueOrg, ordinary.Id, model.GetModeDefault)
	require.NoError(t, err)
	target := base.NativeTarget
	require.Equal(t, baselineReceipt.NativeTarget, target)
	fingerprint := model.ObservationBaselineFingerprint(manifest, tofu, graph, base.ModuleArtifacts, target)
	now := time.Now().UTC()
	request := observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: queueProject, EnvId: queueEnv,
		ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute),
		ExpectedBaselineId: &base.Id, ExpectedBaselineFingerprint: &fingerprint}
	digest, err := model.ObservationRequestDigest(request)
	require.NoError(t, err)
	observationTx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = observationTx.Rollback() }()
	run, err := f.store.CreateDeployment(f.ctx, observationTx, queueOrg, queueProject, queueEnv, model.CreateDeploymentParams{
		NativeTarget: target, CreatedBy: f.actor, DeploymentEnvUuid: request.EnvironmentUuid, Mode: model.DeploymentModeObservation,
		Manifest: manifest, Tofu: tofu, Graph: graph, RunnerId: base.RunnerId, RunnerLogLevel: "info",
		ModuleArtifacts: base.ModuleArtifacts, ModuleVersionRequestDigest: base.ModuleVersionRequestDigest, Metrics: base.Metrics,
	})
	require.NoError(t, err)
	require.False(t, run.HasPipeline)
	require.NoError(t, model.ReserveRunnerJob(f.ctx, observationTx, run, 2))
	accepted := observation.ObservationReceipt{RequestId: request.RequestId, Disposition: observation.ObservationReceiptDispositionAccepted,
		Reason: observation.ObservationReceiptReasonAccepted, RecordedAt: now, RunId: &run.Id, BaselineId: &base.Id, BaselineFingerprint: &fingerprint}
	raw, err := json.Marshal(accepted)
	require.NoError(t, err)
	_, err = observationTx.ExecContext(f.ctx, `INSERT INTO environment_observation_receipts(org_id,plugin_id,request_id,request_sha256,environment_uuid,actor_id,grant_id,grant_revision,receipt,deployment_id,baseline_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		queueOrg, "dev.example.plugin", request.RequestId, digest, request.EnvironmentUuid, f.actor, request.GrantId, request.GrantRevision, raw, run.Id, base.Id)
	require.NoError(t, err)
	require.NoError(t, model.StoreObservationResult(f.ctx, observationTx, run.Id, observation.ObservationResult{SchemaVersion: "1", Complete: true, Tool: "opentofu", ToolVersion: "1.10.0",
		ObservedChanges: []observation.ObservationChange{}, ProposedChanges: []observation.ObservationChange{{Id: "r1", ResourceType: "test_resource", Actions: []observation.ObservationChangeActions{"update"}}}}))
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, observationTx, run.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	require.NoError(t, observationTx.Commit())
	require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations WHERE deployment_id=$1 AND released_at IS NULL`, run.Id))
	require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_leases WHERE deployment_id=$1`, run.Id))
	_, f.private, err = ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.runID = run.Id
	f.body = observation.ObservationRedeployRequest{RequestId: uuid.New(), ProjectId: queueProject, EnvId: queueEnv,
		ProjectUuid: f.core.projectUUID, EnvironmentUuid: f.core.envUUID, BaselineId: base.Id, BaselineFingerprint: fingerprint}
	adapter := wireRemediationTargetAdapter(t, &f, f.server.ControlPlaneClient)
	adapter.Materials = ExternalOperationMaterialService{Vault: managedLogVault(t)}
	response := f.forward(t, f.body)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var result observation.ObservationRedeployReceipt
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	receipt := nativeProducerReceipt(t, f.queueFixture, result.DeploymentId)
	require.Equal(t, target, receipt.NativeTarget)
	require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: baselineReceipt.AdmissionID, Epoch: baselineReceipt.CoreEpoch}, receipt.NativeAdmissionWatermark)
	require.Nil(t, receipt.ExternalDispatch, "drift remediation does not acquire a progressive-rollout reservation")
	child, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, result.DeploymentId, model.GetModeDefault)
	require.NoError(t, err)
	require.NotEmpty(t, child.EncryptedLogsRecipient.Or(""))
	readTx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	logs, err := model.GetDeploymentLogMaterial(f.ctx, readTx, queueOrg, child.Id)
	require.NoError(t, err)
	require.Equal(t, logs.Recipient, child.EncryptedLogsRecipient.Must())
	require.NotEqual(t, child.EncryptedOutputsRecipient.Or(""), logs.Recipient)
	require.NoError(t, readTx.Rollback())
	require.Equal(t, model.PipelineDefault, receipt.Pipeline.ID)
	require.Equal(t, base.Id, f.body.BaselineId)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM environment_observation_redeploy_receipts WHERE deployment_id=$1`, result.DeploymentId))
}
