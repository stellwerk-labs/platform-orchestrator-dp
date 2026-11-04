//go:build integration

package model

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
)

func TestObservationResolutionSealsAbsentAndPreservesExactBinding(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	request, actor := observationRequest(f), uuid.New()
	first, err := ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationResolutionDispositionNotStarted, first.Disposition)
	require.Nil(t, first.Receipt)
	again, err := ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
	require.NoError(t, err)
	require.Equal(t, first, again)
	_, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
	require.ErrorIs(t, err, ErrObservationRequestSealed)
	require.Empty(t, pending)
	changed := request
	changed.EnvironmentUuid = uuid.New()
	_, err = ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, changed)
	_, conflict := IsErrConflict(err)
	require.True(t, conflict)
	_, err = ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), request)
	_, conflict = IsErrConflict(err)
	require.True(t, conflict)
	require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
	require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM environment_observation_request_seals`))
}

func TestObservationResolutionRacesAdmission(t *testing.T) {
	for index := range 10 {
		t.Run(string(rune('a'+index)), func(t *testing.T) {
			f, _ := observationBaselineFixture(t)
			request, actor := observationRequest(f), uuid.New()
			start := make(chan struct{})
			var receipt *observation.ObservationReceipt
			var resolution *observation.ObservationResolution
			var admissionErr, resolutionErr error
			var group sync.WaitGroup
			group.Add(2)
			go func() {
				defer group.Done()
				<-start
				receipt, _, admissionErr = TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
			}()
			go func() {
				defer group.Done()
				<-start
				resolution, resolutionErr = ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
			}()
			close(start)
			group.Wait()
			require.NoError(t, resolutionErr)
			if resolution.Disposition == observation.ObservationResolutionDispositionAccepted {
				require.NoError(t, admissionErr)
				require.NotNil(t, receipt)
				require.Equal(t, receipt, resolution.Receipt)
				require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
				require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
				require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM environment_observation_request_seals`))
			} else {
				require.ErrorIs(t, admissionErr, ErrObservationRequestSealed)
				require.Nil(t, resolution.Receipt)
				require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
				require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
			}
		})
	}
}

func TestObservationResolutionWaitsForUncommittedSealAndSurvivesLostAck(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	request, actor := observationRequest(f), uuid.New()
	digest, err := ObservationRequestDigest(request)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, LockObservationRequest(f.ctx, tx, ObservationRequestKindObservation, "queue-org", "dev.example.plugin", request.RequestId))
	require.NoError(t, sealObservationRequest(f.ctx, tx, ObservationRequestKindObservation, "queue-org", "dev.example.plugin", request.RequestId, actor, uuid.Nil, digest))
	done := make(chan error, 1)
	go func() {
		_, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("forward passed an uncommitted request seal: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, tx.Commit())
	require.ErrorIs(t, <-done, ErrObservationRequestSealed)
	// The first seal acknowledgement can be lost. A replay still settles the
	// exact immutable request without a fabricated Deployment or slot release.
	resolution, err := ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationResolutionDispositionNotStarted, resolution.Disposition)
	require.Nil(t, resolution.Receipt)
	require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
}

func TestObservationResolutionAcceptedNeverReleasesUnknownRunner(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	request, actor := observationRequest(f), uuid.New()
	receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
	require.NoError(t, err)
	dep, _, _, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *receipt.RunId, GetModeDefault)
	require.NoError(t, err)
	_, err = f.store.BeginDeploymentDispatch(f.ctx, dep)
	require.NoError(t, err)
	_, err = f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, *receipt.RunId, UpdateDeploymentStatusAndOutputsParams{Status: DeploymentStatusFailed})
	require.NoError(t, err)
	for range 2 {
		resolution, err := ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
		require.NoError(t, err)
		require.Equal(t, observation.ObservationResolutionDispositionAccepted, resolution.Disposition)
		require.Equal(t, receipt, resolution.Receipt)
	}
	require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM environment_observation_request_seals`))
}

func TestObservationResolutionSkippedRemainsGetVisible(t *testing.T) {
	f, _ := observationBaselineFixture(t)
	request, actor := observationRequest(f), uuid.New()
	request.AdmissionDeadline = time.Now().Add(-time.Second)
	receipt, _, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
	require.NoError(t, err)
	resolution, err := ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationResolutionDispositionNotStarted, resolution.Disposition)
	require.Nil(t, resolution.Receipt)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	stored, err := ReadObservationReceipt(f.ctx, tx, "queue-org", "dev.example.plugin", request.RequestId)
	require.NoError(t, err)
	require.Equal(t, receipt, stored)
}

func TestObservationResolutionArbitratesInflightAdmissionCommitOrCancellation(t *testing.T) {
	for _, cancelAdmission := range []bool{false, true} {
		name := "commit"
		if cancelAdmission {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			f, _ := observationBaselineFixture(t)
			request, actor := observationRequest(f), uuid.New()
			environmentTx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = environmentTx.Rollback() }()
			require.NoError(t, LockEnvironmentMutation(f.ctx, environmentTx, f.env))
			admissionCtx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			admitted := make(chan error, 1)
			go func() {
				_, _, err := TryStartObservation(admissionCtx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
				admitted <- err
			}()
			// The real admission owns its request lock and is blocked at the actual
			// Environment mutation fence. Resolution must wait for commit or rollback.
			require.Eventually(t, func() bool {
				probe, err := f.store.BeginTx(f.ctx, nil)
				require.NoError(t, err)
				var free bool
				err = probe.QueryRowContext(f.ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`, "observation-request:queue-org:dev.example.plugin:"+request.RequestId.String()).Scan(&free)
				require.NoError(t, err)
				require.NoError(t, probe.Rollback())
				return !free
			}, 2*time.Second, 10*time.Millisecond)
			resolved := make(chan *observation.ObservationResolution, 1)
			failed := make(chan error, 1)
			go func() {
				value, err := ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
				resolved <- value
				failed <- err
			}()
			select {
			case <-resolved:
				t.Fatal("resolution bypassed the in-flight admission lock")
			case <-time.After(50 * time.Millisecond):
			}
			if cancelAdmission {
				cancel()
			}
			require.NoError(t, environmentTx.Commit())
			value := <-resolved
			require.NoError(t, <-failed)
			admissionErr := <-admitted
			if cancelAdmission {
				require.Error(t, admissionErr)
				require.Equal(t, observation.ObservationResolutionDispositionNotStarted, value.Disposition)
				_, _, err = TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request, "runner", 2, true)
				require.ErrorIs(t, err, ErrObservationRequestSealed)
				require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM deployments WHERE mode='observation'`))
			} else {
				require.NoError(t, admissionErr)
				require.Equal(t, observation.ObservationResolutionDispositionAccepted, value.Disposition)
				require.NotNil(t, value.Receipt.RunId)
				require.Equal(t, 1, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
			}
		})
	}
}

// An old replica can already be running when another replica migrates the DB.
// Exercise its original transaction shape without calling the new seal check.
func TestObservationResolutionDatabaseGuardBlocksAlreadyRunningLegacyWriters(t *testing.T) {
	for _, kind := range []ObservationRequestKind{ObservationRequestKindObservation, ObservationRequestKindRedeploy} {
		t.Run(string(kind), func(t *testing.T) {
			f, base := observationBaselineFixture(t)
			request, actor, runID := observationRequest(f), uuid.New(), uuid.New()
			var digest string
			if kind == ObservationRequestKindObservation {
				resolution, err := ResolveObservationRequest(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, request)
				require.NoError(t, err)
				require.Equal(t, observation.ObservationResolutionDispositionNotStarted, resolution.Disposition)
				digest, err = ObservationRequestDigest(request)
				require.NoError(t, err)
			} else {
				body := observation.ObservationRedeployRequest{RequestId: request.RequestId, ProjectId: request.ProjectId, EnvId: request.EnvId, ProjectUuid: request.ProjectUuid, EnvironmentUuid: request.EnvironmentUuid, BaselineId: base.Id, BaselineFingerprint: "sha256:" + strings.Repeat("a", 64)}
				resolution, err := ResolveObservationRedeploy(f.ctx, f.store, "queue-org", "dev.example.plugin", actor, runID, body)
				require.NoError(t, err)
				require.Equal(t, observation.ObservationRedeployResolutionDispositionNotStarted, resolution.Disposition)
				digest, err = ObservationRequestDigest(body)
				require.NoError(t, err)
			}
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			require.NoError(t, LockObservationRequest(f.ctx, tx, kind, "queue-org", "dev.example.plugin", request.RequestId))
			mode := DeploymentModeObservation
			if kind == ObservationRequestKindRedeploy {
				mode = DeploymentModeDeploy
			}
			dep, err := f.store.CreateDeployment(
				f.ctx,
				tx,
				"queue-org",
				"p",
				"e",
				CreateDeploymentParams{DeploymentEnvUuid: f.env, Mode: mode, CreatedBy: actor, RunnerId: "runner", RunnerLogLevel: "info", Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), Tofu: RawTofu("terraform {}")},
			)
			require.NoError(t, err)
			require.NoError(t, ReserveRunnerJob(f.ctx, tx, dep, 2))
			_, err = f.store.InsertPendingEventMessages(f.ctx, tx, []*hstandardoutbox.PendingEventMessage{{Subject: "io.platform-orchestrator.deployment.created", Payload: ConvertDeploymentToEventPayload(dep)}})
			require.NoError(t, err)
			if kind == ObservationRequestKindObservation {
				receipt, marshalErr := json.Marshal(observation.ObservationReceipt{RequestId: request.RequestId, Disposition: observation.ObservationReceiptDispositionAccepted, Reason: observation.ObservationReceiptReasonAccepted, RecordedAt: time.Now(), RunId: &dep.Id, BaselineId: &base.Id})
				require.NoError(t, marshalErr)
				_, err = tx.ExecContext(
					f.ctx,
					`INSERT INTO environment_observation_receipts(org_id,plugin_id,request_id,request_sha256,environment_uuid,actor_id,grant_id,grant_revision,receipt,deployment_id,baseline_id) VALUES('queue-org','dev.example.plugin',$1,$2,$3,$4,$5,1,$6,$7,$8)`,
					request.RequestId,
					digest,
					f.env,
					actor,
					request.GrantId,
					receipt,
					dep.Id,
					base.Id,
				)
			} else {
				_, err = tx.ExecContext(
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
			require.True(t, errors.As(err, &rejected), "legacy receipt insert must fail before commit")
			require.Equal(t, "environment_observation_request_not_started", rejected.Constraint)
			require.NoError(t, tx.Rollback())
			require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
			require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM pending_event_messages`))
			head, _, _, _, err := f.store.GetLastDeployment(f.ctx, nil, "queue-org", "p", "e", GetLastDeploymentParams{StateChangeOnly: true})
			require.NoError(t, err)
			require.Equal(t, base.Id, head.Id)
		})
	}
}
