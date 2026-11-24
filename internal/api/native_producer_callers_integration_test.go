//go:build integration

package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/require"
)

// This owned HTTP peer and real PostgreSQL prove caller contracts, not actual Core policy or site execution.
func newNativeProducerFixture(t *testing.T) *queueFixture {
	t.Helper()
	f := newQueueFixture(t, DeploymentQueueConfig{})
	eligible := true
	f.core.set(func(c *queueCore) { c.nativeFirstEligible = &eligible })
	runner := qualifiedNativeRunner(t)
	runner.OrgId, runner.Id = queueOrg, queueRunner
	countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return runner, http.StatusOK })
	require.Nil(t, f.server.RunnerRollouts)
	return f
}

func nativeProducerReceipt(t *testing.T, f *queueFixture, id uuid.UUID) *model.DeploymentAdmissionReceipt {
	t.Helper()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	receipt, err := model.GetDeploymentAdmissionReceiptByDeployment(f.ctx, tx, id)
	require.NoError(t, err)
	return receipt
}

func TestNativeProducerFirstPlanAndApplyUseLatestKnowledgeOnce(t *testing.T) {
	f := newNativeProducerFixture(t)
	f.server.Vault = managedLogVault(t)
	first := f.mustDeploy(queueBody(map[string]any{"plan_only": true, "managed_logs": true}))
	firstReceipt := nativeProducerReceipt(t, f, first.Id)
	require.NotNil(t, firstReceipt.NativeTarget)
	require.Nil(t, firstReceipt.NativeAdmissionWatermark)
	completeNativePlan(t, f, first.Id)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, first.Id))
	second := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	secondReceipt := nativeProducerReceipt(t, f, second.Id)
	require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: firstReceipt.AdmissionID, Epoch: firstReceipt.CoreEpoch}, secondReceipt.NativeAdmissionWatermark)
	completeNativePlan(t, f, second.Id)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, second.Id))
	require.Zero(t, f.count(`SELECT count(*) FROM deployments WHERE mode NOT IN ('plan_only','rollback_plan')`))
	require.Equal(t, "ready", nativePlanState(t, f, first.Id).State)
	f.server.PipelineDispatcher = &PipelineDispatcher{ToolsDigest: "legacy-tools", MetadataKey: "legacy-metadata"}
	child, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, first.Id, "exact-source-subject", "once")
	require.NoError(t, err)
	material := retainedTestLogMaterial(t, f, child)
	parent, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, first.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.NotEqual(t, parent.EncryptedLogsRecipient, child.EncryptedLogsRecipient)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_log_material WHERE deployment_id=$1`, child.Id))
	require.NoError(t, f.store.ReserveRunnerDispatch(f.ctx, child, 1))
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: f.server, Key: key}
	frozen, err := dispatcher.frozenCommand(f.ctx, child)
	require.NoError(t, err)
	payload, err := model.FrozenPipelinePayload(frozen, child)
	require.NoError(t, err)
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(payload, &command))
	require.Equal(t, material.Recipient, command.Request.EncryptedLogsRecipient)
	applied := nativeProducerReceipt(t, f, child.Id)
	require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: secondReceipt.AdmissionID, Epoch: secondReceipt.CoreEpoch}, applied.NativeAdmissionWatermark)
	require.Equal(t, firstReceipt.NativeTarget, applied.NativeTarget)
	require.Equal(t, model.PipelineApplySaved, applied.Pipeline.Strategy)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, first.Id))
	control := f.server.ControlPlaneClient
	logVault := f.server.Vault
	f.server.ControlPlaneClient = nil
	f.server.Vault = nil
	again, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, first.Id, "exact-source-subject", "once")
	f.server.ControlPlaneClient = control
	f.server.Vault = logVault
	require.NoError(t, err)
	require.Equal(t, child.Id, again.Id)
	require.Equal(t, child.EncryptedLogsRecipient, again.EncryptedLogsRecipient)
	_, err = f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, first.Id, "exact-source-subject", "twice")
	require.ErrorContains(t, err, "already used")
}

func TestNativeProducerQueuePromotionDerivesCurrentPredecessor(t *testing.T) {
	f := newNativeProducerFixture(t)
	first := f.mustDeploy(queueBody(nil))
	queued := f.mustQueue(f.deploy(queueBody(map[string]any{"when_busy": "queue"})))
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_admission_receipts`))
	f.complete(first.Id, model.DeploymentStatusSucceeded)
	f.exec(`UPDATE deployment_pipeline_runs SET settled=true WHERE deployment_id=$1`, first.Id)
	f.settle(f.server, first.Id)
	item := f.item(queued.Id)
	require.Equal(t, model.DeploymentQueueDispatched, item.State)
	firstReceipt := nativeProducerReceipt(t, f, first.Id)
	promoted := nativeProducerReceipt(t, f, item.DeploymentID.Must())
	require.Equal(t, firstReceipt.NativeTarget, promoted.NativeTarget)
	require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: firstReceipt.AdmissionID, Epoch: firstReceipt.CoreEpoch}, promoted.NativeAdmissionWatermark)
	require.Equal(t, 2, f.count(`SELECT count(*) FROM deployments`))
	require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations`), "admission does not dispatch or fabricate a capacity slot")
}

func TestNativeProducerFreshEligibilityRefusalAndReadOnlyPreview(t *testing.T) {
	f := newNativeProducerFixture(t)
	for _, eligibility := range []*bool{nil, ref.Ref(false)} {
		f.core.set(func(c *queueCore) { c.nativeFirstEligible = eligibility })
		response := f.deploy(queueBody(map[string]any{"plan_only": true}))
		require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), "not eligible")
	}
	f.core.set(func(c *queueCore) { c.nativeFirstEligible = ref.Ref(true) })
	response := f.deploy(queueBody(map[string]any{"is_dry_run": true}))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Zero(t, f.core.admissions())
	require.Zero(t, f.count(`SELECT count(*) FROM deployment_admission_receipts`))
	require.Zero(t, f.count(`SELECT count(*) FROM deployments`))
}

func TestNativeProducerRawWrapperAndMismatchedAttemptRemainClosed(t *testing.T) {
	f := newNativeProducerFixture(t)
	response, err := f.server.ControlPlaneClient.GetEnvironmentWithResponse(f.ctx, queueOrg, queueProject, queueEnv)
	require.NoError(t, err)
	env := *response.JSON200
	for _, attempt := range []*OrdinaryDeploymentAdmissionAttempt{nil, f.server.DeploymentAdmissions.NewAttempt(queueOrg, *env.ProjectUuid, env.Uuid, uuid.New(), model.DeploymentModeDeploy, "wrong-actor")} {
		tx, err := f.store.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		_, _, _, err = CreateDeploymentWithOptions(
			f.ctx,
			f.server.Logger,
			f.actor,
			queueOrg,
			queueProject,
			queueEnv,
			env,
			model.DeploymentModeDeploy,
			opt.Empty[uuid.UUID](),
			DeploymentManifest{},
			nil,
			nil,
			nil,
			false,
			nil,
			f.server.ControlPlaneClient,
			f.store,
			tx,
			DeploymentOptions{Admission: attempt},
		)
		require.ErrorContains(t, err, "admission")
		require.NoError(t, tx.Rollback())
	}
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, _, _, err = CreateDeployment(f.ctx, f.server.Logger, f.actor, queueOrg, queueProject, queueEnv, env, model.DeploymentModeDeploy, opt.Empty[uuid.UUID](), DeploymentManifest{}, nil, nil, nil, false, nil, f.server.ControlPlaneClient, f.store, tx)
	require.ErrorContains(t, err, "authoritative Core admission")
	require.NoError(t, tx.Rollback())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	attempt := f.server.DeploymentAdmissions.NewAttempt(queueOrg, *env.ProjectUuid, env.Uuid, f.actor, model.DeploymentModeDeploy, "trusted-preview")
	_, _, _, err = CreateDeploymentWithOptions(f.ctx, f.server.Logger, f.actor, queueOrg, queueProject, queueEnv, env, model.DeploymentModeDeploy, opt.Empty[uuid.UUID](), DeploymentManifest{}, nil, nil, nil, true, nil, f.server.ControlPlaneClient, f.store, tx, DeploymentOptions{Admission: attempt})
	require.NoError(t, err, "a trusted preview must not prepare an admission even if an attempt is supplied")
	require.NoError(t, tx.Rollback())
	require.Zero(t, f.core.admissions())
	require.Zero(t, f.count(`SELECT count(*) FROM deployments`))
}

func TestNativeProducerFreshLegacyRetainedPlanRefusesBeforeVault(t *testing.T) {
	f := newNativeProducerFixture(t)
	response, err := f.server.ControlPlaneClient.GetEnvironmentWithResponse(f.ctx, queueOrg, queueProject, queueEnv)
	require.NoError(t, err)
	service := &RetainedPlanService{Server: f.server}
	request := RetainedPlanRequest{OrgID: queueOrg, ProjectID: queueProject, EnvID: queueEnv, Environment: *response.JSON200, Manifest: DeploymentManifest{}, Backend: "s3", Tool: "opentofu"}
	// A nil Vault would panic if the legacy route passed the fresh-Native exclusion.
	require.Nil(t, f.server.Vault)
	for _, populated := range []bool{false, true} {
		if populated {
			plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
			completeNativePlan(t, f, plan.Id)
			require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, plan.Id))
		}
		_, err := service.CreatePlan(f.ctx, f.actor, request, uuid.New())
		require.ErrorContains(t, err, "deployment pipeline Plan API")
	}
	require.Zero(t, f.count(`SELECT count(*) FROM retained_plans`))
}

func TestNativeProducerSystemDestroyUsesFixedPipelineAndCurrentReceipt(t *testing.T) {
	f := newNativeProducerFixture(t)
	base := f.mustDeploy(queueBody(nil))
	f.complete(base.Id, model.DeploymentStatusSucceeded)
	f.exec(`UPDATE deployment_pipeline_runs SET settled=true WHERE deployment_id=$1`, base.Id)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, base.Id))
	f.core.set(func(c *queueCore) { c.status = cp.EnvironmentStatusDeleting })
	response, err := f.server.ControlPlaneClient.GetEnvironmentWithResponse(f.ctx, queueOrg, queueProject, queueEnv)
	require.NoError(t, err)
	env := *response.JSON200
	attempt := f.server.DeploymentAdmissions.NewAttempt(queueOrg, *env.ProjectUuid, env.Uuid, userid.InternalSystemUuid, model.DeploymentModeDestroy, "auto-destroy")
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dep, _, _, err := CreateDeploymentWithOptions(
		f.ctx,
		f.server.Logger,
		userid.InternalSystemUuid,
		queueOrg,
		queueProject,
		queueEnv,
		env,
		model.DeploymentModeDestroy,
		opt.Empty[uuid.UUID](),
		DeploymentManifest{},
		nil,
		nil,
		nil,
		false,
		nil,
		f.server.ControlPlaneClient,
		f.store,
		tx,
		DeploymentOptions{Admission: attempt, ExecutionCatalogue: f.server.DeploymentAdmissions.ExecutionCatalogueClient()},
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	receipt := nativeProducerReceipt(t, f, dep.Id)
	require.Equal(t, userid.InternalSystemUuid, receipt.ActorID)
	require.Equal(t, "destroy", receipt.Pipeline.Strategy)
	require.Equal(t, model.PipelineDefault, receipt.Pipeline.ID)
	require.NotNil(t, receipt.NativeAdmissionWatermark)
	require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_queue`))
}

func TestNativeProducerExternalAttemptPreservesDispatchLineage(t *testing.T) {
	for _, mode := range []model.DeploymentMode{model.DeploymentModeDeploy, model.DeploymentModeRollback} {
		t.Run(string(mode), func(t *testing.T) {
			f := newNativeProducerFixture(t)
			base := f.mustDeploy(queueBody(nil))
			f.complete(base.Id, model.DeploymentStatusSucceeded)
			f.exec(`UPDATE deployment_pipeline_runs SET settled=true WHERE deployment_id=$1`, base.Id)
			require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, base.Id))
			response, err := f.server.ControlPlaneClient.GetEnvironmentWithResponse(f.ctx, queueOrg, queueProject, queueEnv)
			require.NoError(t, err)
			env := response.JSON200
			admission, err := (&ExternalDeploymentHTTPAdapter{Server: f.server, OrdinaryAdmissions: f.server.DeploymentAdmissions}).resolvePipelineAdmission(f.ctx, f.actor, queueOrg, "dev.example.rollouts", uuid.NewString(), env, mode, nil)
			require.NoError(t, err)
			dispatch := &model.ExternalAdmissionDispatch{PluginID: "dev.example.rollouts", OperationID: uuid.New(), DispatchID: uuid.NewString(), Fence: 9, RequestDigest: strings.Repeat("b", 64)}
			admission.Attempt.ExternalDispatch = dispatch
			rollback := opt.Empty[uuid.UUID]()
			if mode == model.DeploymentModeRollback {
				rollback = opt.Of(base.Id)
			}
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			dep, _, _, err := CreateDeploymentWithOptions(
				f.ctx,
				f.server.Logger,
				f.actor,
				queueOrg,
				queueProject,
				queueEnv,
				*env,
				mode,
				rollback,
				DeploymentManifest{},
				nil,
				nil,
				nil,
				false,
				nil,
				f.server.ControlPlaneClient,
				f.store,
				tx,
				DeploymentOptions{Runner: admission.Runner, Pipeline: admission.Definition, Admission: admission.Attempt, ExecutionCatalogue: f.server.DeploymentAdmissions.ExecutionCatalogueClient()},
			)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			receipt := nativeProducerReceipt(t, f, dep.Id)
			require.Equal(t, dispatch, receipt.ExternalDispatch)
			require.NotNil(t, receipt.NativeAdmissionWatermark)
			f.core.mu.Lock()
			wire, err := json.Marshal(f.core.guards[receipt.AdmissionID].request.ExternalDispatch)
			f.core.mu.Unlock()
			require.NoError(t, err)
			require.Contains(t, string(wire), dispatch.OperationID.String())
		})
	}
}

func TestNativeProducerReplanRollsBackBeforeSealingFailedAdmission(t *testing.T) {
	for _, stage := range []string{"after-prepare", "lineage", "commit"} {
		t.Run(stage, func(t *testing.T) {
			f := newNativeProducerFixture(t)
			plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
			completeNativePlan(t, f, plan.Id)
			require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, plan.Id))
			if stage == "after-prepare" {
				peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/orgs/"+queueOrg+"/deployment-admissions/") && !strings.HasSuffix(r.URL.Path, "/seal-absence") {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					f.core.ServeHTTP(w, r)
				}))
				t.Cleanup(peer.Close)
				var err error
				f.server.DeploymentAdmissions, err = NewOrdinaryDeploymentAdmissionService(f.store, peer.URL, f.tokenFile, http.DefaultClient)
				require.NoError(t, err)
			} else {
				f.exec(`CREATE FUNCTION producer_fail_replan() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'owned replan failure'; END; $$`)
				if stage == "lineage" {
					f.exec(`CREATE TRIGGER producer_fail_replan BEFORE UPDATE ON deployment_pipeline_runs FOR EACH ROW WHEN (NEW.replanned_from_id IS NOT NULL) EXECUTE FUNCTION producer_fail_replan()`)
				} else {
					f.exec(`CREATE CONSTRAINT TRIGGER producer_fail_replan AFTER INSERT ON deployments DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION producer_fail_replan()`)
				}
			}
			_, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, stage)
			require.Error(t, err)
			require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
			require.Zero(t, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE replanned_from_id=$1`, plan.Id))
			require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_admission_receipts WHERE disposition='absent' AND core_settled_at IS NOT NULL`))
			require.Empty(t, f.core.unsettledGuards())
			require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations`))
		})
	}
}

func TestNativeProducerAcceptedPlanReplaysBeforeCurrentMetadata(t *testing.T) {
	f := newNativeProducerFixture(t)
	path := "/orgs/" + queueOrg + "/deployments"
	response := f.call(http.MethodPost, path, queueBody(map[string]any{"plan_only": true}), f.actor, "accepted-native-plan")
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	var plan Deployment
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &plan))
	calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return cp.Runner{}, http.StatusServiceUnavailable })
	f.core.set(func(c *queueCore) { c.nativeFirstEligible = nil })
	replay := f.call(http.MethodPost, path, queueBody(map[string]any{"plan_only": true}), f.actor, "accepted-native-plan")
	require.Equal(t, http.StatusCreated, replay.Code, replay.Body.String())
	var again Deployment
	require.NoError(t, json.Unmarshal(replay.Body.Bytes(), &again))
	require.Equal(t, plan.Id, again.Id)
	require.Zero(t, calls.Load())
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
	changed := f.call(http.MethodPost, path, queueBody(map[string]any{"plan_only": true, "managed_logs": true}), f.actor, "accepted-native-plan")
	require.Equal(t, http.StatusConflict, changed.Code, changed.Body.String())
	require.Zero(t, calls.Load())
}

func TestNativeProducerSavedApplyRejectsCurrentToolOrBackendChanges(t *testing.T) {
	for _, changed := range []string{"registration", "tools", "backend"} {
		t.Run(changed, func(t *testing.T) {
			f := newNativeProducerFixture(t)
			plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
			completeNativePlan(t, f, plan.Id)
			require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, plan.Id))
			runner := qualifiedNativeRunner(t)
			runner.OrgId, runner.Id = queueOrg, queueRunner
			switch changed {
			case "registration":
				replacement := uuid.New()
				runner.Uuid = &replacement
			case "tools":
				config, err := runner.RunnerConfiguration.AsNativeAgentRunnerConfiguration()
				require.NoError(t, err)
				config.NativeExecution.ToolsDigest = strings.Repeat("c", 64)
				require.NoError(t, runner.RunnerConfiguration.FromNativeAgentRunnerConfiguration(config))
			case "backend":
				require.NoError(t, runner.StateStorageConfiguration.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: "replacement"}))
			}
			countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return runner, http.StatusOK })
			_, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "changed-target")
			require.ErrorContains(t, err, "execution target changed")
			require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
			require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_admission_receipts`))
		})
	}
}

func TestNativeProducerPlanAndNoEffectAdvanceKnowledgeWithoutReplacingCustody(t *testing.T) {
	f := newNativeProducerFixture(t)
	base := f.mustDeploy(queueBody(nil))
	f.complete(base.Id, model.DeploymentStatusSucceeded)
	f.exec(`UPDATE deployment_pipeline_runs SET settled=true WHERE deployment_id=$1`, base.Id)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, base.Id))
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, plan.Id))
	failed := f.mustDeploy(queueBody(nil))
	stored, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, failed.Id, model.GetModeDefault)
	require.NoError(t, err)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	raw, err := (&PipelineDispatcher{Server: f.server, Key: key}).signCommand(stored, *definition, []byte("owned no-effect projection fixture"), json.RawMessage(`{}`), "s3", nil, nil, "")
	require.NoError(t, err)
	payload, err := model.FrozenPipelinePayload(raw, stored)
	require.NoError(t, err)
	digest := sha256.Sum256(payload)
	f.exec(`UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, failed.Id, raw)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	// Projection proof only. Native event authentication has its separate authority campaign.
	require.NoError(t, model.RecordPipelineNoEffect(f.ctx, tx, stored, hex.EncodeToString(digest[:])))
	require.NoError(t, tx.Commit())
	f.complete(failed.Id, model.DeploymentStatusFailed)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, failed.Id))
	next := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	receipt := nativeProducerReceipt(t, f, next.Id)
	previous := nativeProducerReceipt(t, f, failed.Id)
	require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: previous.AdmissionID, Epoch: previous.CoreEpoch}, receipt.NativeAdmissionWatermark)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	effective, err := model.EffectiveStateDeploymentID(f.ctx, tx, f.core.envUUID)
	require.NoError(t, err)
	require.Equal(t, base.Id, effective)
	capture, _, err := readPipelinePlanCapture(f.ctx, tx, next.Id, false)
	require.NoError(t, err)
	require.Equal(t, base.Id, capture.BaseDeploymentID)
}

func TestNativeProducerSupersededPlanReplansAgainstCurrentBaseAndKnowledge(t *testing.T) {
	f := newNativeProducerFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, plan.Id))
	newer := f.mustDeploy(queueBody(nil))
	f.complete(newer.Id, model.DeploymentStatusSucceeded)
	f.exec(`UPDATE deployment_pipeline_runs SET settled=true WHERE deployment_id=$1`, newer.Id)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, newer.Id))
	_, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "stale")
	require.ErrorContains(t, err, "out of date")
	replanned, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "fresh")
	require.NoError(t, err)
	receipt := nativeProducerReceipt(t, f, replanned.Id)
	preceding := nativeProducerReceipt(t, f, newer.Id)
	require.Equal(t, &cp.NativeAdmissionWatermark{AdmissionId: preceding.AdmissionID, Epoch: preceding.CoreEpoch}, receipt.NativeAdmissionWatermark)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	capture, _, err := readPipelinePlanCapture(f.ctx, tx, replanned.Id, false)
	require.NoError(t, err)
	require.Equal(t, newer.Id, capture.BaseDeploymentID)
	require.Equal(t, receipt.NativeTarget, capture.NativeTarget)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE replanned_from_id=$1`, plan.Id))
}

func TestNativeProducerLegacyRetainedReplayPrecedesChangedRunnerAndVault(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.Vault = managedLogVault(t)
	service := &RetainedPlanService{Server: f.server}
	response, err := f.server.ControlPlaneClient.GetEnvironmentWithResponse(f.ctx, queueOrg, queueProject, queueEnv)
	require.NoError(t, err)
	request := RetainedPlanRequest{OrgID: queueOrg, ProjectID: queueProject, EnvID: queueEnv, Environment: *response.JSON200, Manifest: DeploymentManifest{}, Backend: "kubernetes", Tool: "opentofu"}
	operation := uuid.New()
	plan, err := service.CreatePlan(f.ctx, f.actor, request, operation)
	require.NoError(t, err)
	current := qualifiedNativeRunner(t)
	current.OrgId, current.Id = queueOrg, queueRunner
	calls := countedRunnerPeer(t, f, func(int64) (cp.Runner, int) { return current, http.StatusOK })
	f.server.Vault = nil
	replay, err := service.CreatePlan(f.ctx, f.actor, request, operation)
	require.NoError(t, err)
	require.Equal(t, plan.SourceDeploymentID, replay.SourceDeploymentID)
	require.Zero(t, calls.Load())
	_, err = service.CreatePlan(f.ctx, f.actor, request, uuid.New())
	require.ErrorContains(t, err, "deployment pipeline Plan API")
	require.EqualValues(t, 1, calls.Load())
	require.Equal(t, 1, f.count(`SELECT count(*) FROM retained_plans`))
}
