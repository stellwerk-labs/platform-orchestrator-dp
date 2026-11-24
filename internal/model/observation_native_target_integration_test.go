//go:build integration

package model

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
)

func TestObservationCurrentTargetMustMatchBaselineBeforeExecution(t *testing.T) {
	for _, test := range []struct {
		name                string
		baseline, supported bool
		change              func(**cp.NativeExecutionTargetReference)
		reason              observation.ObservationReceiptReason
	}{
		{"legacy to Native", false, true, nil, observation.ObservationReceiptReasonBaselineChanged},
		{"Native to legacy", true, true, func(p **cp.NativeExecutionTargetReference) { *p = nil }, observation.ObservationReceiptReasonBaselineChanged},
		{"disabled", true, false, nil, observation.ObservationReceiptReasonUnsupported},
		{"Runner ID", true, true, func(p **cp.NativeExecutionTargetReference) { (*p).RunnerId = "another" }, observation.ObservationReceiptReasonBaselineChanged},
		{"registration", true, true, func(p **cp.NativeExecutionTargetReference) { (*p).RunnerUuid = uuid.New() }, observation.ObservationReceiptReasonBaselineChanged},
		{"runtime", true, true, func(p **cp.NativeExecutionTargetReference) { (*p).RuntimeVersion = "other" }, observation.ObservationReceiptReasonBaselineChanged},
		{"protocol", true, true, func(p **cp.NativeExecutionTargetReference) { (*p).PipelineVersion = "other" }, observation.ObservationReceiptReasonBaselineChanged},
		{"tools", true, true, func(p **cp.NativeExecutionTargetReference) { (*p).ToolsDigest = "other" }, observation.ObservationReceiptReasonBaselineChanged},
		{"backend", true, true, func(p **cp.NativeExecutionTargetReference) { (*p).StateBackendType = "gcs" }, observation.ObservationReceiptReasonBaselineChanged},
		{"state configuration", true, true, func(p **cp.NativeExecutionTargetReference) { (*p).StateConfigurationDigest = "other" }, observation.ObservationReceiptReasonBaselineChanged},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, base := observationBaselineFixture(t)
			original := testNativeTarget()
			if test.baseline {
				raw, err := json.Marshal(original)
				require.NoError(t, err)
				_, err = f.store.(*databaser).ExecContext(f.ctx, `UPDATE deployments SET native_target=$2,runner_id=$3 WHERE id=$1`, base.Id, raw, original.RunnerId)
				require.NoError(t, err)
			}
			current := *original
			target := &current
			if test.change != nil {
				test.change(&target)
			}
			before := map[string]int{}
			for _, table := range []string{"deployments", "runner_job_reservations", "environment_deployment_leases", "deployment_dispatch_budgets", "pending_event_messages"} {
				before[table] = readObservationCount(t, f, "SELECT count(*) FROM "+table)
			}
			request, actor := observationRequest(f), uuid.New()
			receipt, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, original.RunnerId, 2, test.supported, target)
			require.NoError(t, err)
			require.Equal(t, observation.ObservationReceiptDispositionSkipped, receipt.Disposition)
			require.Equal(t, test.reason, receipt.Reason)
			require.Nil(t, receipt.RunId)
			require.Empty(t, pending)
			for table, count := range before {
				require.Equal(t, count, readObservationCount(t, f, "SELECT count(*) FROM "+table), table)
			}
			again, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "changed-current", 0, false, nil)
			require.NoError(t, err)
			require.Equal(t, receipt, again, "server qualification is outside caller identity")
			require.Empty(t, pending)
		})
	}
}

func TestNativeObservationCapacityRollbackIsSealed(t *testing.T) {
	f := newQueueModelFixture(t)
	target := testNativeTarget()
	base, err := f.create(t, DeploymentModeDeploy, func(params *CreateDeploymentParams) {
		params.NativeTarget, params.RunnerId = target, target.RunnerId
	})
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	other, err := f.store.CreateDeployment(f.ctx, tx, "queue-org", "p", "other", CreateDeploymentParams{
		NativeTarget: target, DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: target.RunnerId,
		RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"),
	})
	require.NoError(t, err)
	require.NoError(t, ReserveRunnerJob(f.ctx, tx, other, 1))
	require.NoError(t, tx.Commit())
	before := map[string]int{}
	for _, table := range []string{"deployments", "runner_job_reservations", "environment_deployment_leases", "deployment_dispatch_budgets", "pending_event_messages"} {
		before[table] = readObservationCount(t, f, "SELECT count(*) FROM "+table)
	}
	request, actor := observationRequest(f), uuid.New()
	receipt, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, target.RunnerId, 1, true, target)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptReasonRunnerFull, receipt.Reason)
	require.Nil(t, receipt.RunId)
	require.Empty(t, pending)
	for table, count := range before {
		require.Equal(t, count, readObservationCount(t, f, "SELECT count(*) FROM "+table), table)
	}
	latest, _, _, _, err := f.store.GetLastDeployment(f.ctx, nil, "queue-org", "p", "e", GetLastDeploymentParams{})
	require.NoError(t, err)
	require.Equal(t, base.Id, latest.Id)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, other.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	again, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, target.RunnerId, 2, true, target)
	require.NoError(t, err)
	require.Equal(t, receipt, again)
	require.Empty(t, pending)
	require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
}

func TestNativeObservationConcurrentIdenticalRequestHasOneEffect(t *testing.T) {
	f := newQueueModelFixture(t)
	target := testNativeTarget()
	base, err := f.create(t, DeploymentModeDeploy, func(params *CreateDeploymentParams) {
		params.NativeTarget, params.RunnerId = target, target.RunnerId
	})
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	request, actor := observationRequest(f), uuid.New()
	start := make(chan struct{})
	var wg sync.WaitGroup
	type result struct {
		receipt *observation.ObservationReceipt
		events  int
		err     error
	}
	done := make(chan result, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			receipt, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, target.RunnerId, 2, true, target)
			done <- result{receipt, len(pending), err}
		}()
	}
	close(start)
	wg.Wait()
	close(done)
	events := 0
	var first *observation.ObservationReceipt
	for value := range done {
		require.NoError(t, value.err)
		if first == nil {
			first = value.receipt
		} else {
			require.Equal(t, first, value.receipt)
		}
		events += value.events
	}
	require.NotNil(t, first)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, first.Disposition)
	require.Equal(t, &base.Id, first.BaselineId)
	require.Equal(t, 1, events)
	for _, table := range []string{"runner_job_reservations", "deployment_dispatch_budgets"} {
		require.Equal(t, 1, readObservationCount(t, f, "SELECT count(*) FROM "+table+" WHERE deployment_id=$1", *first.RunId), table)
	}
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM pending_event_messages`))
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
	dep, _, _, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *first.RunId, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, target, dep.NativeTarget)
	require.False(t, dep.HasPipeline)
}

func TestObservationTargetCheckUsesTheLockedBaseline(t *testing.T) {
	f, base := observationBaselineFixture(t)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, LockEnvironmentMutation(f.ctx, tx, f.env))
	var holder int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT pg_backend_pid()`).Scan(&holder))
	type result struct {
		receipt *observation.ObservationReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true, nil)
		done <- result{receipt, err}
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		// Each poll needs fresh statistics, not the lock holder's transaction snapshot.
		err := f.store.(*databaser).QueryRowContext(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, holder).Scan(&blocked)
		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
	target := testNativeTarget()
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2,runner_id=$3 WHERE id=$1`, base.Id, raw, target.RunnerId)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	value := <-done
	require.NoError(t, value.err)
	require.Equal(t, observation.ObservationReceiptReasonBaselineChanged, value.receipt.Reason)
	require.Nil(t, value.receipt.RunId)
	require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations`))
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM deployments`))
}

func TestObservationRejectsContradictoryRetainedRunnerIdentity(t *testing.T) {
	f, base := observationBaselineFixture(t)
	target := testNativeTarget()
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	_, err = f.store.(*databaser).ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, base.Id, raw)
	require.NoError(t, err)
	receipt, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), target.RunnerId, 2, true, target)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptReasonBaselineChanged, receipt.Reason)
	require.Empty(t, pending)
	require.Nil(t, receipt.RunId)
}
