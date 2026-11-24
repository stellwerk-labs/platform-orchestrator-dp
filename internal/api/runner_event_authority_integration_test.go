//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// This fixture persists synthetic Native authority. It does not dispatch a v2
// command or qualify its completion/NoEffect decoder, which remains D2-gated.
func nativeEventServer(t *testing.T) *Server {
	t.Helper()
	dsn := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, dsn)
	return &Server{Database: externalDeploymentTestDatabase(t, t.Context(), dsn), Logger: zap.NewNop(), EventPublisher: &hmessaging.RecordingPublisher{}, RunnerTokenSalt: "owned-salt"}
}

func createEventDeployment(t *testing.T, s *Server, target *cp.NativeExecutionTargetReference, pipeline bool) *model.DeploymentSummary {
	t.Helper()
	params := model.CreateDeploymentParams{
		DeploymentEnvUuid: uuid.New(),
		Mode:              model.DeploymentModeDeployPlan,
		CreatedBy:         uuid.New(),
		RunnerId:          "native_pool",
		RunnerLogLevel:    "info",
		Manifest:          model.EncodedDeploymentManifest(`{"workloads":{}}`),
		Graph:             model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`),
		Tofu:              model.RawTofu("terraform {}"),
		NativeTarget:      target,
	}
	if pipeline {
		var err error
		params.Pipeline, err = model.ResolvePipeline(nil, params.Mode)
		require.NoError(t, err)
	}
	tx, err := s.Database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dep, err := s.Database.CreateDeployment(t.Context(), tx, "event-org", "project", uuid.NewString(), params)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return dep
}

func nativeResultEvent(t *testing.T, dep *model.DeploymentSummary, body DeploymentResultsUpdateBody) hmessaging.EventEnvelope {
	t.Helper()
	token := dep.RunnerId
	if dep.NativeTarget != nil {
		var err error
		token, err = hmessaging.NativeRunnerToken(dep.NativeTarget.RunnerUuid.String())
		require.NoError(t, err)
	}
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	return hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, OrganizationID: dep.OrgId, RunnerID: token, DeploymentID: dep.Id.String(), EventID: dep.Id.String() + ":deployment-result", Type: "deployment-result", CreatedAt: time.Now().UTC(), Payload: payload}
}

func eventDatabaseSnapshot(t *testing.T, s *Server) map[string]string {
	t.Helper()
	tx, err := s.Database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(t.Context(), `SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	snapshot := map[string]string{}
	for _, name := range names {
		var content string
		require.NoError(t, tx.QueryRowContext(t.Context(), fmt.Sprintf(`SELECT COALESCE(jsonb_agg(r ORDER BY r::text),'[]'::jsonb)::text FROM (SELECT to_jsonb(t) AS r FROM %q t) q`, name)).Scan(&content))
		snapshot[name] = content
	}
	return snapshot
}

func TestNativeEventAuthorityRefusesBeforeAnyPersistentEffect(t *testing.T) {
	s := nativeEventServer(t)
	dep := createEventDeployment(t, s, nativeTargetFixture(), false)
	legacy := createEventDeployment(t, s, nil, false)
	malformedTarget := nativeTargetFixture()
	malformedTarget.ToolsDigest = "invalid"
	malformed := createEventDeployment(t, s, nativeTargetFixture(), false)
	rawMalformed, err := json.Marshal(malformedTarget)
	require.NoError(t, err)
	corruptTx, err := s.Database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = corruptTx.Rollback() }()
	_, err = corruptTx.ExecContext(t.Context(), `UPDATE deployments SET native_target=$2 WHERE id=$1 AND de_id=$3`, malformed.Id, rawMalformed, malformed.DeploymentEnvUuid)
	require.NoError(t, err)
	require.NoError(t, corruptTx.Commit())
	malformed.NativeTarget = malformedTarget
	body := DeploymentResultsUpdateBody{Status: Success}
	event := nativeResultEvent(t, dep, body)
	for _, test := range []struct {
		name string
		call func() error
	}{
		{"persisted malformed target", func() error {
			e := nativeResultEvent(t, malformed, body)
			return s.ApplyRunnerDeploymentResultEnvelope(t.Context(), e, body)
		}},
		{"domain alias", func() error {
			e := event
			e.RunnerID = dep.RunnerId
			return s.ApplyRunnerDeploymentResultEnvelope(t.Context(), e, body)
		}},
		{"foreign site", func() error {
			e := event
			e.RunnerID, _ = hmessaging.NativeRunnerToken(uuid.NewString())
			return s.ApplyRunnerDeploymentResultEnvelope(t.Context(), e, body)
		}},
		{"foreign organization", func() error {
			e := event
			e.OrganizationID = "foreign"
			return s.ApplyRunnerDeploymentResultEnvelope(t.Context(), e, body)
		}},
		{"same site inner collision", func() error {
			e := event
			e.EventID = legacy.Id.String() + ":deployment-result"
			return s.ApplyRunnerDeploymentResultEnvelope(t.Context(), e, body)
		}},
		{"noncanonical deployment", func() error {
			e := event
			e.DeploymentID = "urn:uuid:" + dep.Id.String()
			return s.ApplyRunnerDeploymentResultEnvelope(t.Context(), e, body)
		}},
		{"bare completion", func() error {
			return s.ApplyRunnerDeploymentResult(t.Context(), dep.OrgId, event.RunnerID, dep.Id, event.EventID, body)
		}},
		{"bare runner error", func() error {
			return s.ApplyRunnerError(t.Context(), dep.OrgId, dep.RunnerId, dep.Id, "error", "failed", "failed")
		}},
		{"native to legacy", func() error {
			e := nativeResultEvent(t, legacy, body)
			e.RunnerID = event.RunnerID
			return s.ApplyRunnerDeploymentResultEnvelope(t.Context(), e, body)
		}},
		{"legacy reserved identity", func() error {
			return s.ApplyRunnerDeploymentResult(t.Context(), legacy.OrgId, legacy.RunnerId, legacy.Id, hmessaging.NativeMessageIDPrefix+"reserved", body)
		}},
		{"native no effect decoder gate", func() error {
			b := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: &PipelineNoEffectReceipt{}}
			e := nativeResultEvent(t, dep, b)
			return s.ApplyRunnerDeploymentResultEnvelope(t.Context(), e, b)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := eventDatabaseSnapshot(t, s)
			err := test.call()
			require.ErrorIs(t, err, ErrRunnerEventTargetInvalid)
			require.Equal(t, before, eventDatabaseSnapshot(t, s))
		})
	}
	before := eventDatabaseSnapshot(t, s)
	response, err := s.UpdateDeploymentResults(t.Context(), UpdateDeploymentResultsRequestObject{OrgId: dep.OrgId, DeploymentId: dep.Id, Body: &body, Params: UpdateDeploymentResultsParams{XDeploymentToken: util.GenerateHashedRunnerToken(s.RunnerTokenSalt, dep.OrgId, dep.Id.String())}})
	require.NoError(t, err)
	require.IsType(t, UpdateDeploymentResults409JSONResponse{}, response)
	require.Equal(t, before, eventDatabaseSnapshot(t, s))
}

func TestNativeScopedInboxSurvivesLegacyPoisonAndReplaysOnce(t *testing.T) {
	s := nativeEventServer(t)
	dep := createEventDeployment(t, s, nativeTargetFixture(), false)
	legacy := createEventDeployment(t, s, nil, false)
	body := DeploymentResultsUpdateBody{Status: Success}
	event := nativeResultEvent(t, dep, body)
	sibling := createEventDeployment(t, s, nativeTargetFixture(), false)
	misbound := nativeResultEvent(t, sibling, body)
	misbound.EventID = event.EventID
	before := eventDatabaseSnapshot(t, s)
	require.ErrorIs(t, s.ApplyRunnerDeploymentResultEnvelope(t.Context(), misbound, body), ErrRunnerEventTargetInvalid)
	require.Equal(t, before, eventDatabaseSnapshot(t, s))
	require.NoError(t, s.ApplyRunnerDeploymentResult(t.Context(), legacy.OrgId, legacy.RunnerId, legacy.Id, event.EventID, body))
	require.NoError(t, s.ApplyRunnerDeploymentResultEnvelope(t.Context(), event, body))
	after := eventDatabaseSnapshot(t, s)
	require.NoError(t, s.ApplyRunnerDeploymentResultEnvelope(t.Context(), event, body))
	require.Equal(t, after, eventDatabaseSnapshot(t, s))
	tx, err := s.Database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	scoped, err := hmessaging.NativeMessageID(dep.OrgId, event.RunnerID, "events", event.Type, event.EventID)
	require.NoError(t, err)
	var runner string
	require.NoError(t, tx.QueryRowContext(t.Context(), `SELECT runner_id FROM runner_event_inbox WHERE event_id=$1 AND deployment_id=$2`, scoped, dep.Id).Scan(&runner))
	require.Equal(t, dep.RunnerId, runner, "capacity joins retain the domain Runner identity")
	var count int
	require.NoError(t, tx.QueryRowContext(t.Context(), `SELECT count(*) FROM runner_event_inbox`).Scan(&count))
	require.Equal(t, 2, count)
}

func TestNativeProgressBindsFrozenSiteDeploymentAndRevision(t *testing.T) {
	s := nativeEventServer(t)
	dep := createEventDeployment(t, s, nativeTargetFixture(), true)
	event := nativeResultEvent(t, dep, DeploymentResultsUpdateBody{Status: Success})
	event.Type = "rollout-progress"
	event.EventID = dep.Id.String() + ":2"
	progress := model.PipelineProgress{ID: dep.Id.String(), EnvironmentID: dep.DeploymentEnvUuid.String(), Revision: 2, Phase: "planned"}
	setPayload := func() {
		var err error
		event.Payload, err = json.Marshal(map[string]any{"progress": progress})
		require.NoError(t, err)
	}
	setPayload()
	for _, change := range []func(){func() { event.RunnerID = dep.RunnerId }, func() { event.EventID = uuid.NewString() + ":2" }, func() { progress.Revision = 1; setPayload() }, func() { progress.ID = uuid.NewString(); setPayload() }, func() { event.ProtocolVersion = "unknown" }, func() { event.Type = "deployment-result" }} {
		originalEvent, originalProgress := event, progress
		change()
		before := eventDatabaseSnapshot(t, s)
		require.ErrorIs(t, s.ApplyPipelineProgressEnvelope(t.Context(), event), ErrRunnerEventTargetInvalid)
		require.Equal(t, before, eventDatabaseSnapshot(t, s))
		event, progress = originalEvent, originalProgress
	}
	require.NoError(t, s.ApplyPipelineProgressEnvelope(t.Context(), event))
	after := eventDatabaseSnapshot(t, s)
	require.NoError(t, s.ApplyPipelineProgressEnvelope(t.Context(), event))
	require.Equal(t, after, eventDatabaseSnapshot(t, s))
	progress.Revision = 1
	event.EventID = dep.Id.String() + ":" + strconv.FormatInt(progress.Revision, 10)
	setPayload()
	require.NoError(t, s.ApplyPipelineProgressEnvelope(t.Context(), event))
	require.Equal(t, after, eventDatabaseSnapshot(t, s), "stale revision does not replace progress")
}

func TestNativeLateCompletionUsesFrozenDomainAuthority(t *testing.T) {
	f := newLateResultFixture(t)
	target := nativeTargetFixture()
	target.RunnerId = f.forward.RunnerId
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, f.forward.Id, raw)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	f.forward.NativeTarget = target
	f.envelope.RunnerID, err = hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	require.NoError(t, err)
	failed := f.fail(t)
	for _, runner := range []string{f.forward.RunnerId, "0-" + uuid.NewString()} {
		event := f.envelope
		event.RunnerID = runner
		before := eventDatabaseSnapshot(t, f.server)
		require.ErrorIs(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, event, f.body), ErrRunnerEventTargetInvalid)
		require.Equal(t, before, eventDatabaseSnapshot(t, f.server))
	}
	// Independently call each transactional late entrypoint, not just its caller.
	foreign := f.envelope
	foreign.EventID = uuid.NewString() + ":deployment-result"
	before := eventDatabaseSnapshot(t, f.server)
	_, err = f.server.retainLateOrdinaryCapacityCompletion(f.ctx, failed, foreign.RunnerID, f.body, &foreign)
	require.ErrorIs(t, err, ErrRunnerEventTargetInvalid)
	require.ErrorIs(t, f.server.retainLateRunnerResult(f.ctx, *failed, foreign.RunnerID, foreign, "unused", f.body), ErrRunnerEventTargetInvalid)
	require.Equal(t, before, eventDatabaseSnapshot(t, f.server))
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.envelope, f.body))
	after := eventDatabaseSnapshot(t, f.server)
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.envelope, f.body))
	require.Equal(t, after, eventDatabaseSnapshot(t, f.server))
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var runner string
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT runner_id FROM deployment_late_runner_results WHERE deployment_id=$1`, f.forward.Id).Scan(&runner))
	require.Equal(t, f.forward.RunnerId, runner)
}

func TestNativeOrdinaryLateCompletionReleasesOnlyOwnedCapacity(t *testing.T) {
	s := nativeEventServer(t)
	dep := createEventDeployment(t, s, nativeTargetFixture(), false)
	other := createEventDeployment(t, s, nativeTargetFixture(), false)
	tx, err := s.Database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, model.ReserveRunnerJob(t.Context(), tx, dep, 2))
	require.NoError(t, model.ReserveRunnerJob(t.Context(), tx, other, 2))
	require.NoError(t, tx.Commit())
	_, err = s.Database.BeginDeploymentDispatch(t.Context(), dep)
	require.NoError(t, err)
	_, err = s.Database.UpdateDeploymentStatusAndOutputs(t.Context(), nil, dep.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed})
	require.NoError(t, err)
	body := DeploymentResultsUpdateBody{Status: Success}
	event := nativeResultEvent(t, dep, body)
	wrong := event
	wrong.EventID = other.Id.String() + ":deployment-result"
	before := eventDatabaseSnapshot(t, s)
	require.ErrorIs(t, s.ApplyRunnerDeploymentResultEnvelope(t.Context(), wrong, body), ErrRunnerEventTargetInvalid)
	require.Equal(t, before, eventDatabaseSnapshot(t, s))
	require.NoError(t, s.ApplyRunnerDeploymentResultEnvelope(t.Context(), event, body))
	after := eventDatabaseSnapshot(t, s)
	require.NoError(t, s.ApplyRunnerDeploymentResultEnvelope(t.Context(), event, body))
	require.Equal(t, after, eventDatabaseSnapshot(t, s))
	tx, err = s.Database.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var released, foreignReleased bool
	require.NoError(t, tx.QueryRowContext(t.Context(), `SELECT released_at IS NOT NULL FROM runner_job_reservations WHERE deployment_id=$1`, dep.Id).Scan(&released))
	require.True(t, released)
	require.NoError(t, tx.QueryRowContext(t.Context(), `SELECT released_at IS NOT NULL FROM runner_job_reservations WHERE deployment_id=$1`, other.Id).Scan(&foreignReleased))
	require.False(t, foreignReleased)
}

func TestNativeOperatorFailureRetainsUnknownOutcomeAndCapacity(t *testing.T) {
	f := newLateResultFixture(t)
	target := nativeTargetFixture()
	target.RunnerId = f.forward.RunnerId
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, f.forward.Id, raw)
	require.NoError(t, err)
	require.NoError(t, model.ReserveRunnerJob(f.ctx, tx, f.forward, 1))
	require.NoError(t, tx.Commit())
	request := InternalForceFailDeploymentRequestObject{OrgId: f.forward.OrgId, DeploymentId: f.forward.Id}
	before := eventDatabaseSnapshot(t, f.server)
	human := context.WithValue(f.ctx, hecho.ContextKeyUserID, userid.NewHumanUserId().String())
	_, err = f.server.InternalForceFailDeployment(human, request)
	require.Error(t, err)
	require.Equal(t, before, eventDatabaseSnapshot(t, f.server))
	system := context.WithValue(f.ctx, hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
	response, err := f.server.InternalForceFailDeployment(system, request)
	require.NoError(t, err)
	require.IsType(t, InternalForceFailDeployment200JSONResponse{}, response)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	evidence, err := model.ReadDeploymentStopEvidence(f.ctx, tx, f.forward.Id)
	require.NoError(t, err)
	require.True(t, evidence.Failed)
	require.True(t, evidence.CapacityReserved)
	require.True(t, evidence.OutcomeUnreconciled())
	require.False(t, evidence.StopProven)
	require.False(t, evidence.Acknowledged)
	fence, err := model.ReadEnvironmentFence(f.ctx, tx, f.forward.Id)
	require.NoError(t, err)
	require.True(t, fence.IsSet())
	require.Equal(t, f.forward.Id, fence.Must().DeploymentID)
	var receipts int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM pipeline_no_effect_receipts`).Scan(&receipts))
	require.Zero(t, receipts)
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_job_completion_receipts`).Scan(&receipts))
	require.Zero(t, receipts)
}
