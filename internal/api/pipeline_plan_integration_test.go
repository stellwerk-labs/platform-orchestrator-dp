//go:build integration

package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"github.com/stellwerk-labs/golib/hmessaging"
	"net/http"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func nativePlanFixture(t *testing.T) *queueFixture {
	t.Helper()
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.server.Vault = managedLogVault(t)
	f.core.set(func(c *queueCore) { c.native = true; c.planApply = true })
	require.Nil(t, f.server.RunnerRollouts)
	return f
}

func completeNativePlan(t *testing.T, f *queueFixture, id uuid.UUID) {
	t.Helper()
	original, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, id, model.GetModeDefault)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	base, _, err := commandPlanAuthority(f.ctx, tx, original)
	require.NoError(t, err)
	_, backend, err := readPipelinePlanCapture(f.ctx, tx, id, false)
	require.NoError(t, err)
	run, err := model.ReadDeploymentPipeline(f.ctx, tx, id)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: f.server, Key: private}
	frozen, err := dispatcher.signCommand(original, run.Definition, []byte("frozen source"), json.RawMessage(`{"workloads":{}}`), backend, base, nil, "")
	require.NoError(t, err)
	f.exec(`UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, id, frozen)
	f.complete(id, model.DeploymentStatusSucceeded)
	f.exec(`UPDATE deployment_pipeline_runs SET settled=true,progress=jsonb_set(jsonb_set(progress,'{phase}','"completed"'),'{subject}','"exact-source-subject"') WHERE deployment_id=$1`, id)
}

func nativePlanState(t *testing.T, f *queueFixture, id uuid.UUID) *pipelinePlanStatus {
	t.Helper()
	response := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployments/"+id.String()+"/pipeline", nil, f.actor, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var view struct {
		Plan *pipelinePlanStatus `json:"plan"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &view))
	require.NotContains(t, response.Body.String(), "plan_capture")
	require.NotContains(t, response.Body.String(), "material_sha256")
	return view.Plan
}

func TestPipelineSavedApplySingleUseAndAcceptedRecovery(t *testing.T) {
	f := nativePlanFixture(t)
	outputs, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true, "managed_logs": true, "encrypted_outputs_recipient": outputs.Recipient().String()}))
	completeNativePlan(t, f, plan.Id)
	require.Equal(t, "ready", nativePlanState(t, f, plan.Id).State, "first-ever empty base is captured, not unknown")
	_, private, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: f.server, Key: private, ToolsDigest: "immutable-tools"}
	original, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, plan.Id, model.GetModeDefault)
	require.NoError(t, err)
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeployPlan)
	require.NoError(t, err)
	frozen, err := dispatcher.signCommand(original, *definition, []byte("opaque frozen closure"), json.RawMessage(`{"workloads":{}}`), "kubernetes", &pipelinePlanBase{BaseDeploymentID: uuid.Nil.String()}, nil, "")
	require.NoError(t, err)
	f.exec(`UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, plan.Id, frozen)

	path := "/orgs/" + queueOrg + "/deployments/" + plan.Id.String() + "/pipeline/actions/apply-plan"
	var group sync.WaitGroup
	ids := make(chan uuid.UUID, 5)
	for range 5 {
		group.Add(1)
		go func() {
			defer group.Done()
			response := f.call(http.MethodPost, path, map[string]string{"subject": "exact-source-subject"}, f.actor, "same-apply")
			if response.Code != http.StatusCreated {
				t.Errorf("Apply: %d %s", response.Code, response.Body.String())
				return
			}
			var child struct {
				ID uuid.UUID `json:"id"`
			}
			if err := json.Unmarshal(response.Body.Bytes(), &child); err != nil {
				t.Error(err)
				return
			}
			ids <- child.ID
		}()
	}
	group.Wait()
	close(ids)
	var child uuid.UUID
	for id := range ids {
		if child == uuid.Nil {
			child = id
		}
		require.Equal(t, child, id)
	}
	require.NotEqual(t, uuid.Nil, child)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	receipt, err := model.GetDeploymentAdmissionReceiptByDeployment(f.ctx, tx, child)
	require.NoError(t, err)
	storedPipeline, err := model.ReadDeploymentPipeline(f.ctx, tx, child)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, &storedPipeline.Definition, receipt.Pipeline, "Core intent must bind the actual child recipe strategy, not the source Plan strategy")

	require.Equal(t, "used", nativePlanState(t, f, plan.Id).State)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, plan.Id))
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, map[string]string{"subject": "exact-source-subject"}, f.actor, "another-apply").Code)
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, map[string]string{"subject": "changed"}, f.actor, "same-apply").Code)
	applied, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, child, model.GetModeDefault)
	require.NoError(t, err)
	planLogMaterial := retainedTestLogMaterial(t, f, original)
	applyLogMaterial := retainedTestLogMaterial(t, f, applied)
	require.NotEqual(t, planLogMaterial.Recipient, applyLogMaterial.Recipient)
	require.NotEqual(t, outputs.Recipient().String(), applyLogMaterial.Recipient)
	require.Equal(t, original.EncryptedOutputsRecipient, applied.EncryptedOutputsRecipient)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_log_material WHERE deployment_id=$1`, child))
	require.Contains(t, applyLogMaterial.EncryptedIdentity, "vault:v")
	require.NotContains(t, applyLogMaterial.EncryptedIdentity, "AGE-SECRET-KEY")
	require.NoError(t, f.store.ReserveRunnerDispatch(f.ctx, applied, 1))
	control := f.server.ControlPlaneClient
	f.server.ControlPlaneClient = nil
	signedChild, err := dispatcher.frozenCommand(f.ctx, applied)
	f.server.ControlPlaneClient = control
	require.NoError(t, err, "saved Apply dispatch must not fetch module material again")
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(signedChild, &envelope))
	var signed struct {
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(signed.Payload, &command))
	require.Equal(t, "apply_saved", command.Strategy)
	require.Equal(t, []byte("opaque frozen closure"), command.Request.Bundle)
	require.Equal(t, plan.Id.String(), command.SourcePlan.SourcePlanID)
	require.Equal(t, uuid.Nil.String(), command.SourcePlan.BaseDeploymentID)
	require.Nil(t, command.PlanBase)

	saved := f.server.ControlPlaneClient
	savedVault := f.server.Vault
	f.server.ControlPlaneClient = nil
	f.server.Vault = nil
	recovered, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "same-apply")
	f.server.ControlPlaneClient = saved
	f.server.Vault = savedVault
	require.NoError(t, err)
	require.Equal(t, child, recovered.Id)
	require.Equal(t, applied.EncryptedLogsRecipient, recovered.EncryptedLogsRecipient)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_log_material WHERE deployment_id=$1`, child))
	f.finish(child, model.DeploymentStatusSucceeded)
	f.exec(`UPDATE deployment_pipeline_runs SET settled=true WHERE deployment_id=$1`, child)
	newer := f.mustDeploy(queueBody(map[string]any{"when_busy": "reject"}))
	require.NotEqual(t, child, newer.Id)
	require.Equal(t, "used", nativePlanState(t, f, plan.Id).State, "own or subsequent head never erases consumption")
}

func TestPipelinePlansPreserveFreshnessUntilMutationAndReplanIsReadOnly(t *testing.T) {
	f := nativePlanFixture(t)
	first := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, first.Id)
	second := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, second.Id)
	require.Equal(t, "ready", nativePlanState(t, f, first.Id).State)
	require.Equal(t, "ready", nativePlanState(t, f, second.Id).State)
	mutation := f.mustDeploy(queueBody(map[string]any{"when_busy": "reject"}))
	f.finish(mutation.Id, model.DeploymentStatusSucceeded)
	f.exec(`UPDATE deployment_pipeline_runs SET settled=true WHERE deployment_id=$1`, mutation.Id)
	stale := nativePlanState(t, f, first.Id)
	require.Equal(t, "superseded", stale.State)
	require.Equal(t, mutation.Id, *stale.CurrentDeploymentID)
	_, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, first.Id, "exact-source-subject", "stale")
	require.ErrorContains(t, err, "out of date")
	replanned, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, first.Id, "new-plan")
	require.NoError(t, err)
	require.Equal(t, model.DeploymentModeDeployPlan, replanned.Mode)
	saved := f.server.ControlPlaneClient
	f.server.ControlPlaneClient = nil
	again, err := f.server.replanPipelinePlan(f.ctx, f.actor, queueOrg, first.Id, "new-plan")
	f.server.ControlPlaneClient = saved
	require.NoError(t, err)
	require.Equal(t, replanned.Id, again.Id)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	capture, _, err := readPipelinePlanCapture(f.ctx, tx, replanned.Id, false)
	require.NoError(t, err)
	require.Equal(t, mutation.Id, capture.BaseDeploymentID)
	base, source, err := commandPlanAuthority(f.ctx, tx, replanned)
	require.NoError(t, err)
	require.Nil(t, source)
	require.Equal(t, mutation.Id.String(), base.BaseDeploymentID)
	require.Equal(t, 0, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, replanned.Id))
}

func TestPipelineSavedApplyRollsBackBindingAndHeadWithOutboxFailure(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	before := f.count(`SELECT count(*) FROM deployments`)
	materials := f.count(`SELECT count(*) FROM deployment_log_material`)
	f.exec(`CREATE FUNCTION refuse_saved_apply_outbox() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'outbox storage fault'; END$$`)
	f.exec(`CREATE TRIGGER refuse_saved_apply_outbox BEFORE INSERT ON pending_event_messages FOR EACH ROW WHEN (NEW.routing_key = 'io.platform-orchestrator.deployment.created') EXECUTE FUNCTION refuse_saved_apply_outbox()`)
	_, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "storage-fault")
	require.ErrorContains(t, err, "outbox storage fault")
	require.Equal(t, before, f.count(`SELECT count(*) FROM deployments`))
	require.Equal(t, materials, f.count(`SELECT count(*) FROM deployment_log_material`))
	require.Equal(t, 0, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, plan.Id))
	require.Equal(t, "ready", nativePlanState(t, f, plan.Id).State)
	require.Empty(t, f.core.unsettledGuards(), "failed admission is durably sealed after transaction rollback")
	f.exec(`DROP TRIGGER refuse_saved_apply_outbox ON pending_event_messages`)
	_, err = f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "retry-after-storage-fault")
	require.NoError(t, err)
	require.Equal(t, materials+1, f.count(`SELECT count(*) FROM deployment_log_material`))
}

func TestPipelineSavedApplyRequiresAtomicManagedLogMaterial(t *testing.T) {
	for _, fault := range []string{"unconfigured Vault", "material insert"} {
		t.Run(fault, func(t *testing.T) {
			f := nativePlanFixture(t)
			plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
			completeNativePlan(t, f, plan.Id)
			deployments := f.count(`SELECT count(*) FROM deployments`)
			materials := f.count(`SELECT count(*) FROM deployment_log_material`)
			messages := f.count(`SELECT count(*) FROM pending_event_messages`)
			logVault := f.server.Vault
			expected := "not configured"
			if fault == "unconfigured Vault" {
				f.server.Vault = nil
			} else {
				expected = "log material storage fault"
				f.exec(`CREATE FUNCTION refuse_saved_apply_material() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'log material storage fault'; END$$`)
				f.exec(`CREATE TRIGGER refuse_saved_apply_material BEFORE INSERT ON deployment_log_material FOR EACH ROW EXECUTE FUNCTION refuse_saved_apply_material()`)
			}
			_, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "material-fault")
			require.ErrorContains(t, err, expected)
			require.Equal(t, deployments, f.count(`SELECT count(*) FROM deployments`))
			require.Equal(t, materials, f.count(`SELECT count(*) FROM deployment_log_material`))
			require.Equal(t, messages, f.count(`SELECT count(*) FROM pending_event_messages`))
			require.Zero(t, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, plan.Id))
			require.Equal(t, "ready", nativePlanState(t, f, plan.Id).State)
			require.Empty(t, f.core.unsettledGuards())
			f.server.Vault = logVault
			if fault == "material insert" {
				f.exec(`DROP TRIGGER refuse_saved_apply_material ON deployment_log_material`)
			}
			child, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "after-material-fault")
			require.NoError(t, err)
			require.True(t, child.EncryptedLogsRecipient.IsSet())
			require.Equal(t, materials+1, f.count(`SELECT count(*) FROM deployment_log_material`))
			require.Equal(t, "used", nativePlanState(t, f, plan.Id).State)
		})
	}
}

func TestPipelineManualPlanCapabilityPreservesOldRunnerWire(t *testing.T) {
	f := nativePlanFixture(t)
	f.core.set(func(c *queueCore) { c.planApply = false })
	legacy := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, legacy.Id)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	capture, _, err := readPipelinePlanCapture(f.ctx, tx, legacy.Id, false)
	require.NoError(t, err)
	require.Nil(t, capture)
	legacyOwner, _, _, _, err := f.store.GetDeployment(f.ctx, tx, queueOrg, legacy.Id, model.GetModeDefault)
	require.NoError(t, err)
	base, source, err := commandPlanAuthority(f.ctx, tx, legacyOwner)
	require.NoError(t, err)
	require.Nil(t, base)
	require.Nil(t, source)
	require.NoError(t, tx.Rollback())
	require.Equal(t, "unavailable", nativePlanState(t, f, legacy.Id).State)
	f.core.set(func(c *queueCore) { c.planApply = true })
	next := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, next.Id)
	require.Equal(t, "ready", nativePlanState(t, f, next.Id).State)
}

func TestPipelineSavedApplyRejectsChangedFrozenMaterialAndBackendTarget(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	f.core.set(func(c *queueCore) { c.storageNamespace = "different-target" })
	_, err := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "target-mismatch")
	require.ErrorContains(t, err, "target changed")
	f.core.set(func(c *queueCore) { c.storageNamespace = "test" })
	f.exec(`UPDATE deployments SET tofu='changed after planning' WHERE id=$1`, plan.Id)
	_, err = f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "material-mismatch")
	require.ErrorContains(t, err, "material changed")
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
	require.Equal(t, 0, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, plan.Id))
}

func TestPipelineSavedApplySerializesWithLateProgress(t *testing.T) {
	f := nativePlanFixture(t)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	progressTx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = progressTx.Rollback() }()
	_, _, _, _, err = f.store.GetDeployment(f.ctx, progressTx, queueOrg, plan.Id, model.GetModeForUpdate)
	require.NoError(t, err)
	var progressPID int
	require.NoError(t, progressTx.QueryRowContext(f.ctx, `SELECT pg_backend_pid()`).Scan(&progressPID))
	_, err = progressTx.ExecContext(f.ctx, `SET LOCAL lock_timeout='2s'`)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, applyErr := f.server.applyPipelinePlan(f.ctx, f.actor, queueOrg, plan.Id, "exact-source-subject", "with-progress")
		result <- applyErr
	}()
	watch, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = watch.Rollback() }()
	require.Eventually(t, func() bool {
		if _, refreshErr := watch.ExecContext(f.ctx, `SELECT pg_stat_clear_snapshot()`); refreshErr != nil {
			return false
		}
		var blocked bool
		queryErr := watch.QueryRowContext(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock')`, progressPID).Scan(&blocked)
		return queryErr == nil && blocked
	}, 5*time.Second, 10*time.Millisecond, "saved Apply waits behind normal Deployment/result locking")
	require.NoError(t, model.UpdatePipelineProgress(f.ctx, progressTx, model.PipelineProgress{ID: plan.Id.String(), EnvironmentID: f.core.envUUID.String(), Phase: "completed", Subject: "exact-source-subject", Revision: 100}))
	require.NoError(t, progressTx.Commit())
	select {
	case applyErr := <-result:
		require.NoError(t, applyErr, "late progress and Apply must not deadlock or seal an otherwise valid key absent")
	case <-f.ctx.Done():
		t.Fatal("saved Apply did not finish after progress committed")
	}
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_pipeline_runs WHERE source_plan_id=$1`, plan.Id))
}
