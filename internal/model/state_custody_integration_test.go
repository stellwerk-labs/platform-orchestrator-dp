//go:build integration

package model

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stretchr/testify/require"
)

func custodyParams() CreateDeploymentParams {
	target := testNativeTarget()
	return CreateDeploymentParams{NativeTarget: target, DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy,
		CreatedBy: uuid.New(), RunnerId: target.RunnerId, RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{}`),
		Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}")}
}

func createCustodyDeployment(t *testing.T, ctx context.Context, store Databaser, env string, params CreateDeploymentParams) (*DeploymentSummary, error) {
	t.Helper()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dep, err := store.CreateDeployment(ctx, tx, "custody-org", "project", env, params)
	if err != nil {
		return nil, err
	}
	require.NoError(t, tx.Commit())
	return dep, nil
}

func stopCustodyDeployment(t *testing.T, ctx context.Context, store Databaser, id uuid.UUID, status DeploymentStatus) {
	t.Helper()
	_, err := store.UpdateDeploymentStatusAndOutputs(ctx, nil, id, UpdateDeploymentStatusAndOutputsParams{
		Status: status, StatusMessage: "recorded Runner completion", RunnerCompletionProven: true})
	require.NoError(t, err)
}

func custodyWriteCounts(t *testing.T, ctx context.Context, store Databaser) map[string]int {
	t.Helper()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	counts := make(map[string]int)
	for _, table := range []string{"deployments", "environment_deployment_leases", "runner_job_reservations", "deployment_dispatch_budgets", "deployment_pipeline_runs", "pending_event_messages"} {
		var count int
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count))
		counts[table] = count
	}
	return counts
}

func TestStateCustodyRefusesUnreviewedTransfersBeforeAnyCreationWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	for _, test := range []struct {
		name   string
		legacy bool
		change func(*CreateDeploymentParams)
	}{
		{"registration", false, func(p *CreateDeploymentParams) { p.NativeTarget.RunnerUuid = uuid.New() }},
		{"backend", false, func(p *CreateDeploymentParams) { p.NativeTarget.StateBackendType = "gcs" }},
		{"state configuration", false, func(p *CreateDeploymentParams) { p.NativeTarget.StateConfigurationDigest = strings.Repeat("c", 64) }},
		{"Native to legacy", false, func(p *CreateDeploymentParams) { p.NativeTarget = nil }},
		{"legacy to Native", true, nil},
		{"malformed current", false, func(p *CreateDeploymentParams) { p.NativeTarget.ToolsDigest = "malformed" }},
		{"domain identity", false, func(p *CreateDeploymentParams) { p.RunnerId = "different-runner" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := custodyParams()
			selected := *params.NativeTarget
			if test.legacy {
				params.NativeTarget = nil
			}
			base, err := createCustodyDeployment(t, ctx, store, test.name, params)
			require.NoError(t, err)
			stopCustodyDeployment(t, ctx, store, base.Id, DeploymentStatusSucceeded)
			params.NativeTarget = &selected
			if test.change != nil {
				test.change(&params)
			}
			before := custodyWriteCounts(t, ctx, store)
			_, err = createCustodyDeployment(t, ctx, store, test.name, params)
			require.ErrorContains(t, err, "explicit handoff")
			require.Equal(t, before, custodyWriteCounts(t, ctx, store))
		})
	}
}

func TestStateCustodyContinuesAcrossTerminalModesAndToolChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	for _, mode := range []DeploymentMode{DeploymentModeDeploy, DeploymentModeDeployPlan, DeploymentModeDestroy} {
		for _, status := range []DeploymentStatus{DeploymentStatusSucceeded, DeploymentStatusFailed} {
			t.Run(string(mode)+"/"+string(status), func(t *testing.T) {
				params := custodyParams()
				params.Mode = mode
				base, err := createCustodyDeployment(t, ctx, store, t.Name(), params)
				require.NoError(t, err)
				stopCustodyDeployment(t, ctx, store, base.Id, status)
				params.Mode = DeploymentModeDeploy
				params.NativeTarget.ToolsDigest = strings.Repeat("c", 64)
				_, err = createCustodyDeployment(t, ctx, store, t.Name(), params)
				require.NoError(t, err, "tool changes do not transfer provider-state custody")
			})
		}
	}
}

func TestStateCustodyUsesOperationalAnchorWhenEffectiveStateIsNull(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	params := custodyParams()
	var err error
	params.Pipeline, err = ResolvePipeline(nil, params.Mode)
	require.NoError(t, err)
	params.PipelineBackend = "s3"
	for i := range 2 {
		dep, err := createCustodyDeployment(t, ctx, store, "no-effect", params)
		require.NoError(t, err)
		// Reproduce an already authenticated retained receipt. Receipt validation
		// and signed Native authority are tested at their actual API boundary.
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET effect_base_captured=true,effect_base_deployment_id=NULL WHERE deployment_id=$1`, dep.Id)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `INSERT INTO pipeline_no_effect_receipts(deployment_id,command_sha256) VALUES($1,$2)`, dep.Id, strings.Repeat("a", 64))
		require.NoError(t, err)
		require.NoError(t, SettleDeploymentPipeline(ctx, tx, dep.Id, DeploymentStatusFailed))
		require.NoError(t, tx.Commit())
		stopCustodyDeployment(t, ctx, store, dep.Id, DeploymentStatusFailed)
		tx, err = store.BeginTx(ctx, nil)
		require.NoError(t, err)
		head, err := EffectiveStateDeploymentID(ctx, tx, params.DeploymentEnvUuid)
		require.NoError(t, err)
		require.Equal(t, uuid.Nil, head, "iteration %d", i)
		require.NoError(t, tx.Rollback())
	}
	changed := *params.NativeTarget
	changed.RunnerUuid = uuid.New()
	params.NativeTarget = &changed
	before := custodyWriteCounts(t, ctx, store)
	_, err = createCustodyDeployment(t, ctx, store, "no-effect", params)
	require.ErrorContains(t, err, "explicit handoff")
	require.Equal(t, before, custodyWriteCounts(t, ctx, store))
}

func TestStateCustodyUsesCurrentStateRatherThanRollbackDesiredGraph(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	params := custodyParams()
	base, err := createCustodyDeployment(t, ctx, store, "rollback", params)
	require.NoError(t, err)
	stopCustodyDeployment(t, ctx, store, base.Id, DeploymentStatusSucceeded)
	latest, err := createCustodyDeployment(t, ctx, store, "rollback", params)
	require.NoError(t, err)
	stopCustodyDeployment(t, ctx, store, latest.Id, DeploymentStatusSucceeded)
	old := *params.NativeTarget
	old.RunnerUuid = uuid.New()
	raw, err := json.Marshal(old)
	require.NoError(t, err)
	_, err = store.(*databaser).ExecContext(ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, base.Id, raw)
	require.NoError(t, err)
	params.Mode, params.RollbackToId = DeploymentModeRollback, opt.Of(base.Id)
	_, err = createCustodyDeployment(t, ctx, store, "rollback", params)
	require.NoError(t, err, "rollback changes desired configuration, never current state location")
}

func TestStateCustodyRefusesAnOlderUnreservedUnknownWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	params := custodyParams()
	older, err := createCustodyDeployment(t, ctx, store, "older", params)
	require.NoError(t, err)
	stopCustodyDeployment(t, ctx, store, older.Id, DeploymentStatusSucceeded)
	latest, err := createCustodyDeployment(t, ctx, store, "older", params)
	require.NoError(t, err)
	stopCustodyDeployment(t, ctx, store, latest.Id, DeploymentStatusSucceeded)
	// Imported older histories may predate both dispatch budgets and capacity
	// reservations. A terminal label alone never proves their effects stopped.
	_, err = store.(*databaser).ExecContext(ctx, `DELETE FROM deployment_dispatch_budgets WHERE deployment_id=$1`, older.Id)
	require.NoError(t, err)
	before := custodyWriteCounts(t, ctx, store)
	_, err = createCustodyDeployment(t, ctx, store, "older", params)
	require.ErrorIs(t, err, ErrOutcomeUnreconciled)
	require.Equal(t, before, custodyWriteCounts(t, ctx, store))
}

func TestStateCustodyBindsTheExactEnvironmentAndAnchor(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	params := custodyParams()
	base, err := createCustodyDeployment(t, ctx, store, "identity", params)
	require.NoError(t, err)
	stopCustodyDeployment(t, ctx, store, base.Id, DeploymentStatusSucceeded)
	for _, test := range []struct {
		org, project, env string
		id                uuid.UUID
	}{
		{"other-org", "project", "identity", params.DeploymentEnvUuid},
		{"custody-org", "other-project", "identity", params.DeploymentEnvUuid},
		{"custody-org", "project", "other-env", params.DeploymentEnvUuid},
		{"custody-org", "project", "identity", uuid.New()},
	} {
		err := RequireStateCustody(ctx, store.(*databaser), test.org, test.project, test.env, test.id, params.RunnerId, params.NativeTarget)
		require.ErrorContains(t, err, "identity is inconsistent")
	}
	other := custodyParams()
	foreign, err := createCustodyDeployment(t, ctx, store, "foreign", other)
	require.NoError(t, err)
	_, err = store.(*databaser).ExecContext(ctx, `UPDATE deployment_environments SET last_state_deployment_id=$2 WHERE id=$1`, params.DeploymentEnvUuid, foreign.Id)
	require.NoError(t, err)
	before := custodyWriteCounts(t, ctx, store)
	_, err = createCustodyDeployment(t, ctx, store, "identity", params)
	require.ErrorContains(t, err, "identity is inconsistent")
	require.Equal(t, before, custodyWriteCounts(t, ctx, store))
}

func TestStateCustodyChecksTheHeadAfterAcquiringTheEnvironmentLock(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	params := custodyParams()
	base, err := createCustodyDeployment(t, ctx, store, "locked", params)
	require.NoError(t, err)
	stopCustodyDeployment(t, ctx, store, base.Id, DeploymentStatusSucceeded)
	holder, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = holder.Rollback() }()
	require.NoError(t, LockEnvironmentMutation(ctx, holder, params.DeploymentEnvUuid))
	var pid int
	require.NoError(t, holder.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid))
	done := make(chan error, 1)
	go func() {
		waiter, err := store.BeginTx(ctx, nil)
		if err != nil {
			done <- err
			return
		}
		defer func() { _ = waiter.Rollback() }()
		_, err = store.CreateDeployment(ctx, waiter, "custody-org", "project", "locked", params)
		if err == nil {
			err = waiter.Commit()
		}
		done <- err
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		require.NoError(t, store.(*databaser).QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked))
		return blocked
	}, 5*time.Second, 10*time.Millisecond)
	changed := *params.NativeTarget
	changed.StateConfigurationDigest = strings.Repeat("c", 64)
	raw, err := json.Marshal(changed)
	require.NoError(t, err)
	_, err = holder.ExecContext(ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, base.Id, raw)
	require.NoError(t, err)
	before := custodyWriteCounts(t, ctx, store)
	require.NoError(t, holder.Commit())
	require.ErrorContains(t, <-done, "explicit handoff")
	require.Equal(t, before, custodyWriteCounts(t, ctx, store))
}

func TestStateCustodyUsesAuthenticatedStopEvidenceForEveryOlderWriter(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	for _, test := range []struct {
		name    string
		proof   string
		stopped bool
	}{
		{"durable attempt", "", false},
		{"reserved attempt", "reservation", false},
		{"wrong Runner inbox", "wrong-runner", false},
		{"wrong organization inbox", "wrong-org", false},
		{"authenticated inbox", "inbox", true},
		{"authenticated completion", "completion", true},
		{"retained late result", "late", true},
		{"audited acknowledgement", "ack", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			params := custodyParams()
			older, err := createCustodyDeployment(t, ctx, store, test.name, params)
			require.NoError(t, err)
			stopCustodyDeployment(t, ctx, store, older.Id, DeploymentStatusFailed)
			latest, err := createCustodyDeployment(t, ctx, store, test.name, params)
			require.NoError(t, err)
			stopCustodyDeployment(t, ctx, store, latest.Id, DeploymentStatusSucceeded)
			db := store.(*databaser)
			_, err = db.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET completion_received_at=NULL,first_attempt_at=now()-interval '2 hours',deadline=now()-interval '1 hour' WHERE deployment_id=$1`, older.Id)
			require.NoError(t, err)
			installCustodyStopEvidence(t, ctx, db, older, test.proof)
			evidence, err := ReadDeploymentStopEvidence(ctx, db, older.Id)
			require.NoError(t, err)
			require.Equal(t, !test.stopped, evidence.OutcomeUnreconciled())
			before := custodyWriteCounts(t, ctx, store)
			_, err = createCustodyDeployment(t, ctx, store, test.name, params)
			if test.stopped {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrOutcomeUnreconciled)
				require.Equal(t, before, custodyWriteCounts(t, ctx, store))
			}
		})
	}
}

func installCustodyStopEvidence(t *testing.T, ctx context.Context, db *databaser, dep *DeploymentSummary, proof string) {
	t.Helper()
	var err error
	switch proof {
	case "reservation":
		_, err = db.ExecContext(ctx, `INSERT INTO runner_job_reservations(deployment_id,org_id,runner_id) VALUES($1,$2,$3)`, dep.Id, dep.OrgId, dep.RunnerId)
	case "completion":
		err = RecordDeploymentCompletion(ctx, db, dep.Id)
	case "inbox", "wrong-runner", "wrong-org":
		org, runner := dep.OrgId, dep.RunnerId
		if proof == "wrong-runner" {
			runner = "other-runner"
		}
		if proof == "wrong-org" {
			org = "other-org"
		}
		_, err = db.ExecContext(ctx, `INSERT INTO runner_event_inbox(event_id,org_id,runner_id,deployment_id,event_type) VALUES($1,$2,$3,$4,'deployment-result')`, uuid.NewString(), org, runner, dep.Id)
	case "late":
		_, err = db.ExecContext(ctx, `INSERT INTO deployment_late_runner_results(deployment_id,event_id,org_id,runner_id,payload_sha256,result_status,occurred_at) VALUES($1,$2,$3,$4,$5,'failure',now())`, dep.Id, uuid.NewString(), dep.OrgId, dep.RunnerId, strings.Repeat("a", 64))
	case "ack":
		_, err = db.ExecContext(ctx, `INSERT INTO deployment_outcome_acknowledgements(deployment_id,org_id,environment_id,plugin_id,operation_ref,actor_id,reason) VALUES($1,$2,$3,'fixture','retained', $4,'operator verified old process stopped')`, dep.Id, dep.OrgId, dep.DeploymentEnvUuid, uuid.New())
	}
	require.NoError(t, err)
}

func TestStateCustodyRefusesMalformedAndDanglingRetainedAuthority(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	params := custodyParams()
	base, err := createCustodyDeployment(t, ctx, store, "malformed", params)
	require.NoError(t, err)
	stopCustodyDeployment(t, ctx, store, base.Id, DeploymentStatusSucceeded)
	for _, malformed := range []string{"dangling anchor", "malformed reference", "domain identity"} {
		t.Run(malformed, func(t *testing.T) {
			tx, err := store.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			if malformed == "dangling anchor" {
				// The existing foreign key is deferred. Check the uncommitted
				// inconsistent authority without weakening or dropping it.
				_, err = tx.ExecContext(ctx, `UPDATE deployment_environments SET last_state_deployment_id=$2 WHERE id=$1`, params.DeploymentEnvUuid, uuid.New())
			} else {
				changed := *params.NativeTarget
				if malformed == "domain identity" {
					changed.RunnerId = "different-runner"
				} else {
					changed.StateConfigurationDigest = "not-a-digest"
				}
				raw, encodeErr := json.Marshal(changed)
				require.NoError(t, encodeErr)
				_, err = tx.ExecContext(ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, base.Id, raw)
			}
			require.NoError(t, err)
			_, err = store.CreateDeployment(ctx, tx, "custody-org", "project", "malformed", params)
			require.ErrorContains(t, err, "explicit handoff")
			var count int
			require.NoError(t, tx.QueryRowContext(ctx, `SELECT count(*) FROM deployments`).Scan(&count))
			require.Equal(t, 1, count)
		})
	}
}
