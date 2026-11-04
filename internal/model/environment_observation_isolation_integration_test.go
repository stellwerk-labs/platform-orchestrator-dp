//go:build integration

package model

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestObservationIsolationRejectsLegacyPreSealFrozenSnapshots(t *testing.T) {
	for _, kind := range []ObservationRequestKind{ObservationRequestKindObservation, ObservationRequestKindRedeploy} {
		t.Run(string(kind), func(t *testing.T) {
			f, base := observationBaselineFixture(t)
			request, actor, runID := observationRequest(f), uuid.New(), uuid.New()
			legacy, err := f.store.BeginTx(f.ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
			require.NoError(t, err)
			defer func() { _ = legacy.Rollback() }()
			var visible int
			require.NoError(t, legacy.QueryRowContext(f.ctx, `SELECT count(*) FROM environment_observation_request_seals`).Scan(&visible))
			require.Zero(t, visible)
			digest, err := ObservationRequestDigest(request)
			require.NoError(t, err)
			if kind == ObservationRequestKindObservation {
				value, err := ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
				require.NoError(t, err)
				require.Equal(t, observation.ObservationResolutionDispositionNotStarted, value.Disposition)
			} else {
				body := observation.ObservationRedeployRequest{RequestId: request.RequestId, ProjectId: request.ProjectId, EnvId: request.EnvId, ProjectUuid: request.ProjectUuid, EnvironmentUuid: request.EnvironmentUuid, BaselineId: base.Id, BaselineFingerprint: "sha256:" + strings.Repeat("a", 64)}
				value, err := ResolveObservationRedeploy(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, runID, body)
				require.NoError(t, err)
				require.Equal(t, observation.ObservationRedeployResolutionDispositionNotStarted, value.Disposition)
				digest, err = ObservationRequestDigest(body)
				require.NoError(t, err)
			}
			require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM environment_observation_request_seals`))
			require.NoError(t, legacy.QueryRowContext(f.ctx, `SELECT count(*) FROM environment_observation_request_seals`).Scan(&visible))
			require.Zero(t, visible, "the actual old transaction snapshot cannot see the committed seal")
			require.ErrorIs(t, LockObservationRequest(f.ctx, legacy, kind, "queue-org", "dev.example.plugin", request.RequestId), ErrObservationIsolationUnsupported)
			// A live old binary does not call the new shared lock guard. Follow its
			// original lock/write sequence against the already-frozen real snapshot.
			prefix := "observation-request:"
			mode := DeploymentModeObservation
			if kind == ObservationRequestKindRedeploy {
				prefix = "observation-redeploy:"
				mode = DeploymentModeDeploy
			}
			_, err = legacy.ExecContext(f.ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, prefix+"queue-org:dev.example.plugin:"+request.RequestId.String())
			require.NoError(t, err)
			dep, err := f.store.CreateDeployment(
				f.ctx,
				legacy,
				"queue-org",
				"p",
				"e",
				CreateDeploymentParams{DeploymentEnvUuid: f.env, Mode: mode, CreatedBy: actor, RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), Tofu: RawTofu("terraform {}")},
			)
			require.NoError(t, err)
			require.NoError(t, ReserveRunnerJob(f.ctx, legacy, dep, 2))
			_, err = f.store.InsertPendingEventMessages(f.ctx, legacy, []*hstandardoutbox.PendingEventMessage{{Subject: "io.platform-orchestrator.deployment.created", Payload: ConvertDeploymentToEventPayload(dep)}})
			require.NoError(t, err)
			if kind == ObservationRequestKindObservation {
				raw, marshalErr := json.Marshal(observation.ObservationReceipt{RequestId: request.RequestId, Disposition: observation.ObservationReceiptDispositionAccepted, Reason: observation.ObservationReceiptReasonAccepted, RecordedAt: time.Now(), RunId: &dep.Id, BaselineId: &base.Id})
				require.NoError(t, marshalErr)
				_, err = legacy.ExecContext(
					f.ctx,
					`INSERT INTO environment_observation_receipts(org_id,plugin_id,request_id,request_sha256,environment_uuid,actor_id,grant_id,grant_revision,receipt,deployment_id,baseline_id) VALUES('queue-org','dev.example.plugin',$1,$2,$3,$4,$5,1,$6,$7,$8)`,
					request.RequestId,
					digest,
					f.env,
					actor,
					request.GrantId,
					raw,
					dep.Id,
					base.Id,
				)
			} else {
				_, err = legacy.ExecContext(
					f.ctx,
					`INSERT INTO environment_observation_redeploy_receipts(org_id,plugin_id,request_id,run_id,actor_id,request_sha256,deployment_id,baseline_id) VALUES('queue-org','dev.example.plugin',$1,$2,$3,$4,$5,$6)`,
					request.RequestId,
					runID,
					actor,
					digest,
					dep.Id,
					base.Id,
				)
			}
			var rejected *pq.Error
			require.True(t, errors.As(err, &rejected), "the real legacy receipt INSERT must fail even when its snapshot misses the seal")
			require.Equal(t, "25000", string(rejected.Code))
			require.Equal(t, "environment_observation_read_committed_required", rejected.Constraint)
			require.NoError(t, legacy.Rollback())
			require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
			require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM pending_event_messages`))
			head, _, _, _, err := f.store.GetLastDeployment(f.ctx, nil, "queue-org", "p", "e", GetLastDeploymentParams{StateChangeOnly: true})
			require.NoError(t, err)
			require.Equal(t, base.Id, head.Id)
		})
	}
}

func TestObservationIsolationRejectsInheritedRepeatableReadForwardsAndResolvers(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	_, _, scoped := migrationTestDatabase(t, ctx, connection)
	normal, err := NewDatabaser(ctx, zap.NewNop(), scoped)
	require.NoError(t, err)
	defer func() { require.NoError(t, normal.Close()) }()
	frozen, err := NewDatabaser(ctx, zap.NewNop(), scoped+" default_transaction_isolation='repeatable read'")
	require.NoError(t, err)
	defer func() { require.NoError(t, frozen.Close()) }()
	f := queueModelFixture{ctx: ctx, store: normal, env: uuid.New()}
	base, err := f.create(t, DeploymentModeDeploy, nil)
	require.NoError(t, err)
	_, err = normal.UpdateDeploymentStatusAndOutputs(ctx, nil, base.Id, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	request, actor, runID := observationRequest(f), uuid.New(), uuid.New()
	snapshot, err := frozen.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = snapshot.Rollback() }()
	var isolation string
	var visible int
	require.NoError(t, snapshot.QueryRowContext(ctx, `SELECT current_setting('transaction_isolation'),count(*) FROM environment_observation_receipts WHERE request_id=$1`, request.RequestId).Scan(&isolation, &visible))
	require.Equal(t, "repeatable read", isolation)
	require.Zero(t, visible)
	receipt, pending, err := TryStartObservation(ctx, normal, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	require.Len(t, pending, 1)
	require.NoError(t, snapshot.QueryRowContext(ctx, `SELECT count(*) FROM environment_observation_receipts WHERE request_id=$1`, request.RequestId).Scan(&visible))
	require.Zero(t, visible, "a resolver with this real pre-admission snapshot would falsely see absence after commit")
	require.ErrorIs(t, LockObservationRequest(ctx, snapshot, ObservationRequestKindObservation, "queue-org", "dev.example.plugin", request.RequestId), ErrObservationIsolationUnsupported)
	require.NoError(t, snapshot.Rollback())
	// Exercise the actual public model entry points with an inherited database
	// default, not merely a direct schema/source check or explicit TxOptions.
	value, err := ResolveObservationRequest(ctx, frozen, "queue-org", "dev.example.plugin", actor, request)
	require.ErrorIs(t, err, ErrObservationIsolationUnsupported)
	require.Nil(t, value)
	redeploy := observation.ObservationRedeployRequest{RequestId: uuid.New(), ProjectId: request.ProjectId, EnvId: request.EnvId, ProjectUuid: request.ProjectUuid, EnvironmentUuid: request.EnvironmentUuid, BaselineId: base.Id, BaselineFingerprint: "sha256:" + strings.Repeat("a", 64)}
	replay, err := ResolveObservationRedeploy(ctx, frozen, "queue-org", "dev.example.plugin", actor, runID, redeploy)
	require.ErrorIs(t, err, ErrObservationIsolationUnsupported)
	require.Nil(t, replay)
	newRequest := request
	newRequest.RequestId = uuid.New()
	_, pending, err = TryStartObservation(ctx, frozen, "queue-org", "dev.example.plugin", actor, newRequest, "runner", 2, true)
	require.ErrorIs(t, err, ErrObservationIsolationUnsupported)
	require.Empty(t, pending)
	require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM environment_observation_request_seals`))
	resolved, err := ResolveObservationRequest(ctx, normal, "queue-org", "dev.example.plugin", actor, request)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationResolutionDispositionAccepted, resolved.Disposition)
	require.Equal(t, receipt, resolved.Receipt)
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
}
