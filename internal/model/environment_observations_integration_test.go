//go:build integration

package model

import (
	"database/sql"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	platformgraph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"
)

func observationRequest(f queueModelFixture) observation.ObservationRequest {
	now := time.Now().UTC()
	return observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: "p", EnvId: "e", ProjectUuid: uuid.New(), EnvironmentUuid: f.env, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)}
}
func observationBaselineFixture(t *testing.T) (queueModelFixture, *DeploymentSummary) {
	t.Helper()
	f := newQueueModelFixture(t)
	base, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	return f, base
}
func readObservationCount(t *testing.T, f queueModelFixture, query string, args ...any) int {
	t.Helper()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var count int
	require.NoError(t, tx.QueryRowContext(f.ctx, query, args...).Scan(&count))
	return count
}

func TestObservationAdmissionReplayAndExactBaseline(t *testing.T) {
	f, base := observationBaselineFixture(t)
	request := observationRequest(f)
	actor := uuid.New()
	receipt, messages, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	require.Equal(t, base.Id, *receipt.BaselineId)
	require.Len(t, messages, 1)
	state, _, _, _, err := f.store.GetLastDeployment(f.ctx, nil, "queue-org", "p", "e", GetLastDeploymentParams{StateChangeOnly: true})
	require.NoError(t, err)
	require.Equal(t, base.Id, state.Id)
	dep, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *receipt.RunId, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, DeploymentModeObservation, dep.Mode)
	require.Equal(t, "info", dep.RunnerLogLevel, "observations emit sanitized aggregate cost entries")
	require.False(t, dep.EncryptedLogsRecipient.IsSet(), "observations do not persist customer logs")
	require.Equal(t, EncodedDeploymentManifest(`{"workloads":{}}`), manifest)
	require.Equal(t, RawTofu("terraform {}"), tofu)
	require.JSONEq(t, `{"nodes":{},"edges":{}}`, string(graph))
	again, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 0, false)
	require.NoError(t, err)
	require.Equal(t, receipt, again)
	require.Empty(t, pending)
	changed := request
	changed.GrantRevision++
	_, _, err = TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, changed, "runner", 2, true)
	require.Error(t, err)
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	// Every ordinary mode shares the observation exclusion, even public plan-only.
	for _, mode := range []DeploymentMode{DeploymentModeDeploy, DeploymentModeDeployPlan, DeploymentModeRollbackPlan, DeploymentModeDestroy} {
		_, err := f.create(t, mode, nil)
		require.Error(t, err)
	}
}

func TestObservationBusyExpiredAndCapacitySkipsAreSealed(t *testing.T) {
	for _, reason := range []observation.ObservationReceiptReason{observation.ObservationReceiptReasonBusy, observation.ObservationReceiptReasonExpired, observation.ObservationReceiptReasonRunnerFull, observation.ObservationReceiptReasonUnsupported} {
		t.Run(string(reason), func(t *testing.T) {
			f, _ := observationBaselineFixture(t)
			request := observationRequest(f)
			actor := uuid.New()
			limit, supported := 1, true
			switch reason {
			case observation.ObservationReceiptReasonBusy:
				_, _, err := f.enqueue(t, nil)
				require.NoError(t, err)
			case observation.ObservationReceiptReasonExpired:
				request.AdmissionDeadline = time.Now().Add(-time.Second)
			case observation.ObservationReceiptReasonUnsupported:
				limit = 0
			case observation.ObservationReceiptReasonRunnerFull:
				otherEnv := uuid.New()
				tx, err := f.store.BeginTx(f.ctx, nil)
				require.NoError(t, err)
				other, err := f.store.CreateDeployment(
					f.ctx,
					tx,
					"queue-org",
					"p",
					"other",
					CreateDeploymentParams{DeploymentEnvUuid: otherEnv, Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}")},
				)
				require.NoError(t, err)
				require.NoError(t, ReserveRunnerJob(f.ctx, tx, other, 1))
				require.NoError(t, tx.Commit())
			}
			receipt, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", limit, supported)
			require.NoError(t, err)
			require.Equal(t, reason, receipt.Reason)
			require.Nil(t, receipt.RunId)
			require.Empty(t, pending)
			require.Equal(t, 0, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
			again, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 100, true)
			require.NoError(t, err)
			require.Equal(t, receipt, again)
		})
	}
}

func TestObservationConcurrentAdmissionAndEnqueue(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	actor := uuid.New()
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan *observation.ObservationReceipt, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, observationRequest(f), "runner", 2, true)
			if err != nil {
				errs <- err
			} else {
				results <- receipt
			}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	accepted := 0
	for receipt := range results {
		if receipt.Disposition == observation.ObservationReceiptDispositionAccepted {
			accepted++
		}
	}
	require.Equal(t, 1, accepted)
	_, _, err := f.enqueue(t, nil)
	require.NoError(t, err)
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM environment_deployment_queue WHERE state='queued'`))
}

func TestRunnerCapacityConcurrentReservationsAndUnknownOutcome(t *testing.T) {
	f := newQueueModelFixture(t)
	var deps []*DeploymentSummary
	for index := range 8 {
		tx, err := f.store.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		dep, err := f.store.CreateDeployment(
			f.ctx,
			tx,
			"queue-org",
			"p",
			uuid.NewString(),
			CreateDeploymentParams{DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}")},
		)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		deps = append(deps, dep)
		_ = index
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan error, len(deps))
	for _, dep := range deps {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; results <- f.store.ReserveRunnerDispatch(f.ctx, dep, 2) }()
	}
	close(start)
	wg.Wait()
	close(results)
	admitted := 0
	for err := range results {
		if err == nil {
			admitted++
		} else {
			require.ErrorIs(t, err, ErrRunnerCapacityFull)
		}
	}
	require.Equal(t, 2, admitted)
	require.Equal(t, 2, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	var owned *DeploymentSummary
	for _, dep := range deps {
		if f.store.ReserveRunnerDispatch(f.ctx, dep, 2) == nil {
			owned = dep
			break
		}
	}
	require.NotNil(t, owned)
	deadline, err := f.store.BeginDeploymentDispatch(f.ctx, owned)
	require.NoError(t, err)
	require.False(t, deadline.IsZero())
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, owned.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed})
	require.NoError(t, err)
	require.Equal(t, 2, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	require.NoError(t, RecordDeploymentCompletion(f.ctx, tx, owned.Id))
	require.NoError(t, tx.Commit())
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	require.Error(t, f.store.ReserveRunnerDispatch(f.ctx, owned, 2))
}

func TestObservationUnknownCompletionFencesAndLateReceiptReleases(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	request := observationRequest(f)
	receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), request, "runner", 2, true)
	require.NoError(t, err)
	dep, _, _, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *receipt.RunId, GetModeDefault)
	require.NoError(t, err)
	_, err = f.store.BeginDeploymentDispatch(f.ctx, dep)
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed})
	require.NoError(t, err)
	_, err = f.create(t, DeploymentModeDeploy, nil)
	requireConflictCode(t, err, CodeOutcomeUnreconciled)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	body := map[string]any{"status": "failure"}
	replayed, err := RecordObservationCompletion(f.ctx, tx, dep.Id, body)
	require.NoError(t, err)
	require.False(t, replayed)
	require.NoError(t, tx.Commit())
	_, err = f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	replayed, err = RecordObservationCompletion(f.ctx, tx, dep.Id, body)
	require.NoError(t, err)
	require.True(t, replayed)
	require.NoError(t, tx.Commit())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = RecordObservationCompletion(f.ctx, tx, dep.Id, map[string]any{"status": "success"})
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
}

func TestObservationSafeResultRetention(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
	require.NoError(t, err)
	result := observation.ObservationResult{
		SchemaVersion:   "1",
		Complete:        true,
		Tool:            "terraform",
		ToolVersion:     "1.9.0",
		ObservedChanges: []observation.ObservationChange{},
		ProposedChanges: []observation.ObservationChange{{Id: "r1", ResourceType: "random_id", Actions: []observation.ObservationChangeActions{"update"}}},
	}
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	require.NoError(t, StoreObservationResult(f.ctx, tx, *receipt.RunId, result))
	require.NoError(t, tx.Commit())
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, *receipt.RunId, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	run, err := ReadObservationRun(f.ctx, tx, "queue-org", "dev.example.plugin", *receipt.RunId)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationRunStatusSucceeded, run.Status)
	require.Equal(t, &result, run.Result)
	require.NoError(t, tx.Rollback())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE environment_observation_receipts SET result_expires_at=now()-interval '1 second'`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	run, err = ReadObservationRun(f.ctx, tx, "queue-org", "dev.example.plugin", *receipt.RunId)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationRunStatusExpired, run.Status)
	require.Nil(t, run.Result)
	_, err = ReadObservationRun(f.ctx, tx, "other-org", "dev.example.plugin", *receipt.RunId)
	require.Error(t, err)
	invalid := result
	invalid.ProposedChanges = []observation.ObservationChange{{Id: "secret-address[password]", ResourceType: "random_id", Actions: []observation.ObservationChangeActions{"update"}}}
	require.Error(t, ValidateObservationResult(invalid))
	raw, _ := json.Marshal(result)
	require.NotContains(t, string(raw), "password")
}

func TestObservationCannotOvertakeUncommittedEnqueue(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, _, err = EnqueueDeployment(f.ctx, tx, NewDeploymentQueueItem{OrgID: "queue-org", ProjectID: "p", EnvID: "e", EnvironmentUUID: f.env, Mode: DeploymentModeDeploy, Intent: json.RawMessage(`{}`), CreatedBy: uuid.New(), TTL: time.Hour}, 3)
	require.NoError(t, err)
	type result struct {
		receipt *observation.ObservationReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
		done <- result{receipt, err}
	}()
	select {
	case value := <-done:
		t.Fatalf("admission escaped the uncommitted Environment queue lock: %+v", value)
	case <-time.After(40 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	value := <-done
	require.NoError(t, value.err)
	require.Equal(t, observation.ObservationReceiptReasonBusy, value.receipt.Reason)
}

func TestObservationConcurrentIdenticalRequestHasOneEffect(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	request := observationRequest(f)
	actor := uuid.New()
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
			receipt, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
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
	require.Equal(t, 1, events)
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
}

func TestObservationRequiresCurrentSuccessfulMutationSnapshot(t *testing.T) {
	for _, mode := range []DeploymentMode{DeploymentModeRollback, DeploymentModeDestroy, DeploymentModeDeploy} {
		t.Run(string(mode), func(t *testing.T) {
			f, base := observationBaselineFixture(t)
			next, err := f.create(t, mode, func(params *CreateDeploymentParams) {
				if mode == DeploymentModeRollback {
					params.RollbackToId = opt.Of(base.Id)
				}
			})
			require.NoError(t, err)
			status := DeploymentStatusSucceeded
			if mode == DeploymentModeDeploy {
				status = DeploymentStatusFailed
			}
			_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, next.Id, UpdateDeploymentStatusAndOutputsParams{Status: status, RunnerCompletionProven: true})
			require.NoError(t, err)
			receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
			require.NoError(t, err)
			if mode == DeploymentModeRollback {
				require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
				require.Equal(t, next.Id, *receipt.BaselineId)
			} else {
				require.Equal(t, observation.ObservationReceiptReasonNoBaseline, receipt.Reason)
				require.Nil(t, receipt.BaselineId)
			}
		})
	}
}

func TestRunnerCapacityAttestationIsExactAndWaitsForClosedBudget(t *testing.T) {
	f := newQueueModelFixture(t)
	dep, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	require.NoError(t, f.store.ReserveRunnerDispatch(f.ctx, dep, 1))
	_, err = f.store.BeginDeploymentDispatch(f.ctx, dep)
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed})
	require.NoError(t, err)
	actor := uuid.New()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, _, err = AcknowledgeDeploymentOutcome(f.ctx, tx, "queue-org", dep.Id, actor, "job authoritatively deleted", time.Now())
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours',deadline=now()-interval '1 hour' WHERE deployment_id=$1`, dep.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	ack, replayed, err := AcknowledgeDeploymentOutcome(f.ctx, tx, "queue-org", dep.Id, actor, "job authoritatively deleted", time.Now())
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, dep.Id, ack.DeploymentID)
	require.NoError(t, tx.Commit())
	require.Equal(t, 0, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, replayed, err = AcknowledgeDeploymentOutcome(f.ctx, tx, "queue-org", dep.Id, actor, "job authoritatively deleted", time.Now())
	require.NoError(t, err)
	require.True(t, replayed)
	require.NoError(t, tx.Commit())
}

func TestRunnerCapacityLimitReductionDrainsWithoutRevokingOwnedJobs(t *testing.T) {
	f := newQueueModelFixture(t)
	var deps []*DeploymentSummary
	for range 3 {
		tx, err := f.store.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		dep, err := f.store.CreateDeployment(
			f.ctx,
			tx,
			"queue-org",
			"p",
			uuid.NewString(),
			CreateDeploymentParams{DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}")},
		)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		deps = append(deps, dep)
	}
	for _, dep := range deps[:2] {
		require.NoError(t, f.store.ReserveRunnerDispatch(f.ctx, dep, 2))
		_, err := f.store.BeginDeploymentDispatch(f.ctx, dep)
		require.NoError(t, err)
	}
	require.NoError(t, f.store.ReserveRunnerDispatch(f.ctx, deps[0], 1), "already owned work survives limit reduction")
	require.ErrorIs(t, f.store.ReserveRunnerDispatch(f.ctx, deps[2], 1), ErrRunnerCapacityFull)
	for index, dep := range deps[:2] {
		_, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
		require.NoError(t, err)
		if index == 0 {
			require.ErrorIs(t, f.store.ReserveRunnerDispatch(f.ctx, deps[2], 1), ErrRunnerCapacityFull)
		}
	}
	require.NoError(t, f.store.ReserveRunnerDispatch(f.ctx, deps[2], 1))
}

func TestObservationCapturesModuleArtifactsAndRejectsMissingIdentity(t *testing.T) {
	f := newQueueModelFixture(t)
	coordinate := platformgraph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.database"}
	rawGraph, err := graphs.ToJson(
		&platformgraph.Graph[*graphs.GraphNodeModuleConfig]{
			Nodes: map[platformgraph.ResourceCoordinate]platformgraph.ResourceNode[*graphs.GraphNodeModuleConfig]{coordinate: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "example", VersionId: "1.0.0", HasInlineSource: true}}},
			Edges: map[platformgraph.ResourceCoordinate]map[string]platformgraph.ResourceCoordinate{},
		},
	)
	require.NoError(t, err)
	artifact := ModuleArtifactRequirement{ModuleID: "example", Version: "1.0.0", ModuleUUID: uuid.New(), VersionUUID: uuid.New(), Source: "inline"}
	base, err := f.create(t, DeploymentModeDeploy, func(params *CreateDeploymentParams) {
		params.Graph = rawGraph
		params.ModuleArtifacts = []ModuleArtifactRequirement{artifact}
		params.ModuleVersionRequestDigest = strings.Repeat("b", 64)
	})
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	dep, _, _, capturedGraph, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *receipt.RunId, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, []ModuleArtifactRequirement{artifact}, dep.ModuleArtifacts)
	require.Equal(t, EncodedDeploymentGraph(rawGraph), capturedGraph)
	require.Equal(t, strings.Repeat("b", 64), dep.ModuleVersionRequestDigest)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET module_artifacts='[]'::jsonb WHERE id=$1`, base.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	unavailable, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptReasonNoBaseline, unavailable.Reason)
}

func TestObservationDeletionCannotEraseLiveRunnerOwnership(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	err = f.store.DeleteDeploymentsForEnv(f.ctx, tx, "queue-org", "p", "e", true, opt.Empty[uuid.UUID]())
	requireConflictCode(t, err, CodeOutcomeUnreconciled)
	require.NoError(t, tx.Rollback())
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, *receipt.RunId, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	require.NoError(t, f.store.DeleteDeploymentsForEnv(f.ctx, tx, "queue-org", "p", "e", true, opt.Empty[uuid.UUID]()))
	require.NoError(t, tx.Commit())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	run, err := ReadObservationRun(f.ctx, tx, "queue-org", "dev.example.plugin", *receipt.RunId)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationRunStatusExpired, run.Status)
	retained, err := ReadObservationReceipt(f.ctx, tx, "queue-org", "dev.example.plugin", receipt.RequestId)
	require.NoError(t, err)
	require.Equal(t, receipt, retained)
}

func TestObservationReadProjectsStaleAndUntrustworthyCurrentBaselines(t *testing.T) {
	for _, nextStatus := range []DeploymentStatus{DeploymentStatusSucceeded, DeploymentStatusFailed} {
		t.Run(string(nextStatus), func(t *testing.T) {
			f, _ := observationBaselineFixture(t)
			receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
			require.NoError(t, err)
			result := observation.ObservationResult{SchemaVersion: "1", Complete: true, Tool: "terraform", ToolVersion: "1.9.0", ObservedChanges: []observation.ObservationChange{}, ProposedChanges: []observation.ObservationChange{}}
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			require.NoError(t, StoreObservationResult(f.ctx, tx, *receipt.RunId, result))
			require.NoError(t, tx.Commit())
			_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, *receipt.RunId, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
			require.NoError(t, err)
			next, err := f.create(t, DeploymentModeDeploy, nil)
			require.NoError(t, err)
			_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, next.Id, UpdateDeploymentStatusAndOutputsParams{Status: nextStatus, RunnerCompletionProven: true})
			require.NoError(t, err)
			tx, err = f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			run, err := ReadObservationRun(f.ctx, tx, "queue-org", "dev.example.plugin", *receipt.RunId)
			require.NoError(t, err)
			if nextStatus == DeploymentStatusSucceeded {
				require.Equal(t, observation.ObservationRunStatusStale, run.Status)
			} else {
				require.Equal(t, observation.ObservationRunStatusUnavailable, run.Status)
			}
			require.Equal(t, &result, run.Result, "safe result remains available as history, never as a fresh clean outcome")
			unchanged, _, _, _, err := f.store.GetDeployment(f.ctx, tx, "queue-org", *receipt.RunId, GetModeDefault)
			require.NoError(t, err)
			require.Equal(t, DeploymentStatusSucceeded, unchanged.Status)
		})
	}
}

func TestRunnerCapacityLateOrdinaryCompletionRetainsOnlyStopEvidence(t *testing.T) {
	f := newQueueModelFixture(t)
	dep, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	require.NoError(t, f.store.ReserveRunnerDispatch(f.ctx, dep, 1))
	_, err = f.store.BeginDeploymentDispatch(f.ctx, dep)
	require.NoError(t, err)
	failed, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed, StatusMessage: "watchdog timeout"})
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	handled, err := RecordLateOrdinaryRunnerCompletion(f.ctx, tx, failed, map[string]any{"status": "success", "outputs": "SECRET_ENCRYPTED_CANARY"})
	require.NoError(t, err)
	require.True(t, handled)
	require.NoError(t, tx.Commit())
	require.Equal(t, 0, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	unchanged, _, _, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", dep.Id, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, DeploymentStatusFailed, unchanged.Status)
	require.Equal(t, "watchdog timeout", unchanged.StatusMessage)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	handled, err = RecordLateOrdinaryRunnerCompletion(f.ctx, tx, failed, map[string]any{"status": "success", "outputs": "SECRET_ENCRYPTED_CANARY"})
	require.NoError(t, err)
	require.True(t, handled)
	require.NoError(t, tx.Commit())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = RecordLateOrdinaryRunnerCompletion(f.ctx, tx, failed, map[string]any{"status": "failure"})
	require.Error(t, err)
	require.NoError(t, tx.Rollback())
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var record string
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT row_to_json(receipt)::text FROM runner_job_completion_receipts receipt WHERE deployment_id=$1`, dep.Id).Scan(&record))
	require.NotContains(t, record, "SECRET_ENCRYPTED_CANARY")
	require.NotContains(t, record, "outputs")
}

func TestObservationDeadlineCannotExpireWhileWaitingForRunnerReservation(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	holder, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback() }()
	_, err = holder.ExecContext(f.ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "runner-job:queue-org:runner")
	require.NoError(t, err)
	request := observationRequest(f)
	request.AdmissionDeadline = time.Now().UTC().Add(300 * time.Millisecond)
	type result struct {
		receipt *observation.ObservationReceipt
		err     error
	}
	done := make(chan result, 1)
	go func() {
		receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), request, "runner", 2, true)
		done <- result{receipt, err}
	}()
	select {
	case value := <-done:
		t.Fatalf("observation must wait for the Runner admission transaction: %+v", value)
	case <-time.After(400 * time.Millisecond):
	}
	require.NoError(t, holder.Commit())
	value := <-done
	require.NoError(t, value.err)
	require.Equal(t, observation.ObservationReceiptReasonExpired, value.receipt.Reason)
	require.Nil(t, value.receipt.RunId)
	require.Equal(t, 0, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
	require.Equal(t, 0, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
}

func TestObservationReadinessFencesUnknownOwnershipUntilAuditedStop(t *testing.T) {
	f, base := observationBaselineFixture(t)
	request := observationRequest(f)
	receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), request, "runner", 2, true)
	require.NoError(t, err)
	refs := []EnvironmentRef{{ProjectID: "p", EnvID: "e"}}
	lookup := func() ([]EnvironmentDeploymentReadiness, error) {
		tx, err := f.store.BeginTx(f.ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		return f.store.GetEnvironmentDeploymentReadiness(f.ctx, tx, "queue-org", refs)
	}

	running, err := lookup()
	require.NoError(t, err)
	require.Len(t, running, 1)
	require.True(t, running[0].Executing)
	require.Equal(t, base.Id, running[0].LatestDeploymentID.Must())
	dep, _, _, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *receipt.RunId, GetModeDefault)
	require.NoError(t, err)
	_, err = f.store.BeginDeploymentDispatch(f.ctx, dep)
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, dep.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed})
	require.NoError(t, err)
	blocked, err := lookup()
	requireConflictCode(t, err, CodeOutcomeUnreconciled)
	require.Nil(t, blocked)
	_, err = f.create(t, DeploymentModeDeploy, nil)
	requireConflictCode(t, err, CodeOutcomeUnreconciled)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours',deadline=now()-interval '1 hour' WHERE deployment_id=$1`, dep.Id)
	require.NoError(t, err)
	_, _, err = AcknowledgeDeploymentOutcome(f.ctx, tx, "queue-org", dep.Id, uuid.New(), "authoritatively stopped observation Job", time.Now())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	ready, err := lookup()
	require.NoError(t, err)
	require.Len(t, ready, 1)
	require.False(t, ready[0].Executing)
	require.Equal(t, DeploymentStatusSucceeded, ready[0].LatestStatus.Must())
	require.Equal(t, base.Id, ready[0].EffectiveBaseDeploymentID.Must())
	_, err = f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
}

func TestObservationRejectsOlderUnknownOrdinaryOwnerBehindNewerSuccess(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	older, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	require.NoError(t, f.store.ReserveRunnerDispatch(f.ctx, older, 2))
	_, err = f.store.BeginDeploymentDispatch(f.ctx, older)
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, older.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed})
	require.NoError(t, err)
	newer, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err, "existing ordinary replacement policy remains unchanged")
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, newer.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	actor := uuid.New()
	request := observationRequest(f)
	skipped, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptReasonOutcomeUnreconciled, skipped.Reason)
	require.Nil(t, skipped.RunId)
	require.Empty(t, pending)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours',deadline=now()-interval '1 hour' WHERE deployment_id=$1`, older.Id)
	require.NoError(t, err)
	_, _, err = AcknowledgeDeploymentOutcome(f.ctx, tx, "queue-org", older.Id, uuid.New(), "authoritatively deleted older ordinary Job", time.Now())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	replayed, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
	require.NoError(t, err)
	require.Equal(t, skipped, replayed, "sealed skip is never resurrected")
	request.RequestId = uuid.New()
	accepted, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, accepted.Disposition)
	require.Equal(t, newer.Id, *accepted.BaselineId)
}

func TestObservationInlineNoDigestDoesNotRelaxExternalOrDraftProvenance(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*ModuleArtifactRequirement, *graphs.GraphNodeModuleConfig)
	}{
		{"external without digest", func(a *ModuleArtifactRequirement, c *graphs.GraphNodeModuleConfig) {
			a.Source = "git::https://example.invalid/module"
			c.HasInlineSource = false
		}},
		{"missing module UUID", func(a *ModuleArtifactRequirement, _ *graphs.GraphNodeModuleConfig) { a.ModuleUUID = uuid.Nil }},
		{"missing version UUID", func(a *ModuleArtifactRequirement, _ *graphs.GraphNodeModuleConfig) { a.VersionUUID = uuid.Nil }},
		{"inline flag disagrees", func(_ *ModuleArtifactRequirement, c *graphs.GraphNodeModuleConfig) { c.HasInlineSource = false }},
		{"different coordinate", func(a *ModuleArtifactRequirement, _ *graphs.GraphNodeModuleConfig) { a.Version = "2.0.0" }},
		{"Draft snapshot", func(a *ModuleArtifactRequirement, _ *graphs.GraphNodeModuleConfig) {
			a.DraftTest = &graphs.DraftTestReference{}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newQueueModelFixture(t)
			config := &graphs.GraphNodeModuleConfig{DefinitionId: "example", VersionId: "1.0.0", HasInlineSource: true}
			artifact := ModuleArtifactRequirement{ModuleID: "example", Version: "1.0.0", ModuleUUID: uuid.New(), VersionUUID: uuid.New(), Source: "inline"}
			test.change(&artifact, config)
			coordinate := platformgraph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.database"}
			raw, err := graphs.ToJson(
				&platformgraph.Graph[*graphs.GraphNodeModuleConfig]{
					Nodes: map[platformgraph.ResourceCoordinate]platformgraph.ResourceNode[*graphs.GraphNodeModuleConfig]{coordinate: {ModuleConfiguration: config}},
					Edges: map[platformgraph.ResourceCoordinate]map[string]platformgraph.ResourceCoordinate{},
				},
			)
			require.NoError(t, err)
			base, err := f.create(t, DeploymentModeDeploy, func(p *CreateDeploymentParams) {
				p.Graph = raw
				p.ModuleArtifacts = []ModuleArtifactRequirement{artifact}
			})
			if artifact.DraftTest != nil {
				require.Error(t, err, "ordinary deployments must not acquire Draft test provenance")
				return
			}
			require.NoError(t, err)
			_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
			require.NoError(t, err)
			receipt, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
			require.NoError(t, err)
			require.Equal(t, observation.ObservationReceiptReasonNoBaseline, receipt.Reason)
			require.Nil(t, receipt.RunId)
			require.Empty(t, pending)
		})
	}
}

func TestObservationPinnedGitNoDigestAdmitsExactRetainedSelector(t *testing.T) {
	f := newQueueModelFixture(t)
	source := "git::https://example.invalid/modules.git//storage?ref=" + strings.Repeat("a", 40)
	artifact := ModuleArtifactRequirement{ModuleID: "example", Version: "1.0.0", ModuleUUID: uuid.New(), VersionUUID: uuid.New(), Source: source}
	coordinate := platformgraph.ResourceCoordinate{Type: "database", Class: "default", Id: "shared.database"}
	graph, err := graphs.ToJson(
		&platformgraph.Graph[*graphs.GraphNodeModuleConfig]{
			Nodes: map[platformgraph.ResourceCoordinate]platformgraph.ResourceNode[*graphs.GraphNodeModuleConfig]{coordinate: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "example", VersionId: "1.0.0"}}},
			Edges: map[platformgraph.ResourceCoordinate]map[string]platformgraph.ResourceCoordinate{},
		},
	)
	require.NoError(t, err)
	base, err := f.create(t, DeploymentModeDeploy, func(p *CreateDeploymentParams) {
		p.Graph = graph
		p.ModuleArtifacts = []ModuleArtifactRequirement{artifact}
		p.Tofu = RawTofu(`module "example" { source = "` + source + `" }`)
	})
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), observationRequest(f), "runner", 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	actual, _, tofu, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *receipt.RunId, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, []ModuleArtifactRequirement{artifact}, actual.ModuleArtifacts)
	require.Equal(t, RawTofu(`module "example" { source = "`+source+`" }`), tofu)
}

func TestDeploymentDeletionFencesRecreatedEnvironmentUUID(t *testing.T) {
	f, old := observationBaselineFixture(t)
	removeOld, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	require.NoError(t, f.store.DeleteDeploymentsForEnv(f.ctx, removeOld, "queue-org", "p", "e", true, opt.Of(f.env)))
	require.NoError(t, removeOld.Commit())
	f.env = uuid.New()
	base, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	queued, _, err := f.enqueue(t, nil)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	err = f.store.DeleteDeploymentsForEnv(f.ctx, tx, "queue-org", "p", "e", true, opt.Of(old.DeploymentEnvUuid))
	_, conflict := IsErrConflict(err)
	require.True(t, conflict, "an old deletion wake must not delete a replacement Environment")
	retained, _, _, _, err := f.store.GetDeployment(f.ctx, tx, "queue-org", base.Id, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, f.env, retained.DeploymentEnvUuid)
	item, err := GetDeploymentQueueItem(f.ctx, tx, "queue-org", queued.ID)
	require.NoError(t, err)
	require.Equal(t, f.env, item.EnvironmentUUID)
	require.NoError(t, f.store.DeleteDeploymentsForEnv(f.ctx, tx, "queue-org", "p", "e", true, opt.Of(f.env)))
	_, _, _, _, err = f.store.GetDeployment(f.ctx, tx, "queue-org", base.Id, GetModeDefault)
	_, missing := IsErrNotFound(err)
	require.True(t, missing)
}
