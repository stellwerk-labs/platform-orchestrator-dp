//go:build integration

package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hnats"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Real PostgreSQL + generated typed HTTP handlers. IAM/Core peers are explicit
// HTTP wire fixtures, not evidence of authorization by a deployed IAM/Core.
func nativeControlFixture(t *testing.T) (*queueFixture, *model.DeploymentSummary, string) {
	t.Helper()
	f := newQueueFixture(t, DeploymentQueueConfig{})
	target := nativeTargetFixture()
	target.RunnerId = queueRunner
	target.RunnerUuid = uuid.New()
	target.StateBackendType = "kubernetes"
	definition, err := model.ResolvePipeline(&model.PipelineSelection{ID: model.PipelineReviewed, Version: 1}, model.DeploymentModeDeploy)
	require.NoError(t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	dep, err := f.store.CreateDeployment(
		f.ctx,
		tx,
		queueOrg,
		queueProject,
		queueEnv,
		model.CreateDeploymentParams{
			NativeTarget:      target,
			DeploymentEnvUuid: f.core.envUUID,
			CreatedBy:         f.actor,
			Mode:              model.DeploymentModeDeploy,
			RunnerId:          queueRunner,
			RunnerLogLevel:    "info",
			Manifest:          model.EncodedDeploymentManifest(`{"workloads":{}}`),
			Graph:             model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`),
			Tofu:              model.RawTofu("terraform {}"),
			Pipeline:          definition,
			PipelineBackend:   "kubernetes",
		},
	)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := &PipelineDispatcher{Server: f.server, Key: key}
	frozen, err := dispatcher.signCommand(dep, *definition, []byte("private frozen source"), json.RawMessage(`{"workloads":{}}`), "kubernetes", nil, nil, "")
	require.NoError(t, err)
	f.exec(`UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, dep.Id, frozen)
	f.server.PipelineDispatcher = dispatcher
	return f, dep, "/orgs/" + queueOrg + "/deployments/" + dep.Id.String() + "/pipeline/controls"
}

func deleteControlDeployment(t *testing.T, f *queueFixture) {
	t.Helper()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	require.NoError(t, f.store.DeleteDeploymentsForEnv(f.ctx, tx, queueOrg, queueProject, queueEnv, true, opt.Of(f.core.envUUID)))
	require.NoError(t, tx.Commit())
	require.Zero(t, f.count(`SELECT count(*) FROM deployments`))
}

func controlResponse(t *testing.T, response *httptest.ResponseRecorder) model.PipelineControl {
	t.Helper()
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	requirePipelineSchema(t, "DeploymentPipelineControlReceipt", response.Body.Bytes())
	var receipt model.PipelineControl
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.NotEqual(t, uuid.Nil, receipt.ID)
	require.NotContains(t, response.Body.String(), "private frozen source")
	require.NotContains(t, response.Body.String(), "command")
	return receipt
}

func TestPipelineControlHTTPAtomicIntentReplayAndCurrentIAM(t *testing.T) {
	f, dep, path := nativeControlFixture(t)
	body := map[string]any{"action": "approve", "subject": strings.Repeat("a", 64)}
	first := controlResponse(t, f.call(http.MethodPost, path, body, f.actor, "same-key"))
	replies := make(chan *httptest.ResponseRecorder, 4)
	for range 4 {
		go func() { replies <- f.call(http.MethodPost, path, body, f.actor, "same-key") }()
	}
	for range 4 {
		replay := controlResponse(t, <-replies)
		require.Equal(t, first.ID, replay.ID)
	}

	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_pipeline_controls`))
	require.Zero(t, f.count(`SELECT count(*) FROM pending_event_messages WHERE routing_key='io.platform-orchestrator.pipeline.control.requested'`))
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`), "controls never create executions")
	require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations`), "no capacity reservation")
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "same-key").Code)
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, map[string]any{"action": "approve", "subject": strings.Repeat("b", 64)}, f.actor, "same-key").Code)
	otherActor := uuid.New()
	other := controlResponse(t, f.call(http.MethodPost, path, body, otherActor, "same-key"))
	require.NotEqual(t, first.ID, other.ID)
	require.Equal(t, otherActor, other.ActorID)
	f.iam.deny(f.actor, true)
	require.Equal(t, http.StatusForbidden, f.call(http.MethodPost, path, body, f.actor, "same-key").Code)
	require.Equal(t, http.StatusForbidden, f.call(http.MethodGet, path+"/"+first.ID.String(), nil, f.actor, "").Code)
	f.iam.deny(f.actor, false)
	reader := uuid.New()
	f.iam.onlyDeploymentRead(reader)
	require.Equal(t, http.StatusOK, f.call(http.MethodGet, path+"/"+first.ID.String(), nil, reader, "").Code)
	require.Equal(t, http.StatusForbidden, f.call(http.MethodPost, path, body, reader, "reader").Code)
	for _, actor := range []uuid.UUID{uuid.Nil, userid.InternalSystemUuid} {
		require.Equal(t, http.StatusUnauthorized, f.call(http.MethodPost, path, body, actor, "identity").Code)
		require.Equal(t, http.StatusUnauthorized, f.call(http.MethodGet, path+"/"+first.ID.String(), nil, actor, "").Code)
	}
	request := f.request(http.MethodPost, path, body, f.actor, "identity")
	request.Header.Set("Po-Org-Id", queueOrg)
	require.Equal(t, http.StatusForbidden, f.serve(request).Code)
	request = f.request(http.MethodPost, path, body, f.actor, "origin")
	request.Header.Set("Origin", "https://foreign.example")
	require.Equal(t, http.StatusForbidden, f.serve(request).Code)
	require.Equal(t, http.StatusNotFound, f.call(http.MethodGet, strings.Replace(path, queueOrg, "other-org", 1)+"/"+first.ID.String(), nil, f.actor, "").Code)
	require.Equal(t, http.StatusNotFound, f.call(http.MethodGet, strings.Replace(path, dep.Id.String(), uuid.NewString(), 1)+"/"+first.ID.String(), nil, f.actor, "").Code)
	deleteControlDeployment(t, f)
	replay := controlResponse(t, f.call(http.MethodPost, path, body, f.actor, "same-key"))
	require.Equal(t, first.ID, replay.ID, "retained replay outlives normal Deployment cleanup")
	require.Equal(t, http.StatusOK, f.call(http.MethodGet, path+"/"+first.ID.String(), nil, f.actor, "").Code)
}

func TestPipelineControlRollsBackOutboxFailure(t *testing.T) {
	f, _, path := nativeControlFixture(t)
	f.exec(`ALTER TABLE pending_event_messages RENAME TO unavailable_outbox`)
	response := f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "rollback")
	require.Equal(t, http.StatusInternalServerError, response.Code, response.Body.String())
	require.Zero(t, f.count(`SELECT count(*) FROM deployment_pipeline_controls`), "receipt and event must commit atomically")
	f.exec(`ALTER TABLE unavailable_outbox RENAME TO pending_event_messages`)
	controlResponse(t, f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "rollback"))
}

func TestPipelineControlExpiredReplayAndLegacyRefusal(t *testing.T) {
	f, dep, path := nativeControlFixture(t)
	original := controlResponse(t, f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "retained"))
	// Retained signed times are changed together only to construct an expired
	// original fixture. This never edits a live site's history or key.
	var frozen []byte
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT command FROM deployment_pipeline_runs WHERE deployment_id=$1`, dep.Id).Scan(&frozen))
	require.NoError(t, tx.Rollback())
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(frozen, &envelope))
	var signed struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	var command map[string]any
	require.NoError(t, json.Unmarshal(signed.Payload, &command))
	command["issued_at"] = time.Now().UTC().Add(-2 * time.Hour)
	command["expires_at"] = time.Now().UTC().Add(-time.Hour)
	signed.Payload, err = json.Marshal(command)
	require.NoError(t, err)
	signed.Signature = base64.StdEncoding.EncodeToString(ed25519.Sign(f.server.PipelineDispatcher.Key, signed.Payload))
	envelope.CreatedAt = command["issued_at"].(time.Time)
	envelope.ExpiresAt = command["expires_at"].(time.Time)
	envelope.Payload, err = json.Marshal(signed)
	require.NoError(t, err)
	frozen, err = json.Marshal(envelope)
	require.NoError(t, err)
	f.exec(`UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, dep.Id, frozen)
	replay := controlResponse(t, f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "retained"))
	require.Equal(t, original.ID, replay.ID)
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "late-new").Code)
	repairBody := map[string]any{"action": "repair", "failed_step": "bootstrap", "revision": 7}
	late := controlResponse(t, f.call(http.MethodPost, path, repairBody, f.actor, "late-repair"))
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	retained, err := model.ReadPipelineControl(f.ctx, tx, queueOrg, dep.Id, late.ID)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	originalPayload := append([]byte(nil), signed.Payload...)
	var outer hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(retained.Command, &outer))
	require.NoError(t, json.Unmarshal(outer.Payload, &signed))
	var request model.PipelineControlRequest
	require.NoError(t, json.Unmarshal(signed.Payload, &request))
	require.Equal(t, pipelineCommandTTL, request.ExpiresAt.Sub(request.IssuedAt))
	require.Equal(t, outer.ExpiresAt, request.ExpiresAt)
	require.Equal(t, *dep.NativeTarget, request.Target)
	require.Equal(t, pipelineControlDigest(originalPayload), request.ExecutionDigest)
	f.iam.deny(f.actor, true)
	require.Equal(t, http.StatusForbidden, f.call(http.MethodPost, path, repairBody, f.actor, "late-repair").Code)
	f.iam.deny(f.actor, false)
	f.exec(`UPDATE deployments SET native_target=NULL WHERE id=$1`, dep.Id)
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "legacy").Code)
}

func TestPipelineControlNATSOutboxReplaysExactFrozenBytes(t *testing.T) {
	f, dep, path := nativeControlFixture(t)
	// Test schemas share the real stream, including its message-ID deduplication.
	outboxNamespace := uuid.New()
	outboxStart := int64(binary.BigEndian.Uint64(outboxNamespace[:8])&((1<<62)-1)) + 1
	f.exec(`SELECT setval(pg_get_serial_sequence('pending_event_messages', 'id'), $1, false)`, outboxStart)
	brokerURL := os.Getenv("OUTCOME_ACK_TEST_NATS_URL")
	require.NotEmpty(t, brokerURL, "explicit isolated application broker required")
	connection, err := nats.Connect(brokerURL, nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	broker, err := jetstream.New(connection)
	require.NoError(t, err)
	f.server.EventPublisher = hnats.NewPublisher(broker, hmessaging.EventsStreamName, zap.NewNop())
	f.server.PipelineDispatcher.Publisher = hnats.NewPublisher(broker, hmessaging.RunnerCommandsStreamName, zap.NewNop())
	legacy, err := connection.JetStream()
	require.NoError(t, err)
	body := map[string]any{"action": "cancel"}
	receipt := controlResponse(t, f.call(http.MethodPost, path, body, f.actor, "outbox"))
	published, err := legacy.GetLastMsg(hmessaging.EventsStreamName, model.PipelineControlOutboxSubject)
	require.NoError(t, err, "acceptance publishes immediately without a scheduled or manual flush")
	var event map[string]any
	require.NoError(t, json.Unmarshal(published.Data, &event))
	require.Equal(t, map[string]any{"org_id": queueOrg, "deployment_id": dep.Id.String(), "control_id": receipt.ID.String()}, event)
	require.Zero(t, f.count(`SELECT count(*) FROM pending_event_messages`))
	replayed := controlResponse(t, f.call(http.MethodPost, path, body, f.actor, "outbox"))
	require.Equal(t, receipt.ID, replayed.ID)
	afterReplay, err := legacy.GetLastMsg(hmessaging.EventsStreamName, model.PipelineControlOutboxSubject)
	require.NoError(t, err)
	require.Equal(t, published.Sequence, afterReplay.Sequence, "accepted replay enqueues no second event")
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	retained, err := model.ReadPipelineControl(f.ctx, tx, queueOrg, dep.Id, receipt.ID)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	delivery := hmessaging.Delivery{Message: hmessaging.Message{Subject: model.PipelineControlOutboxSubject, Data: published.Data}}
	require.NoError(t, f.server.PipelineDispatcher.DispatchControl(f.ctx, zap.NewNop(), delivery))
	route, err := hmessaging.NativeRunnerToken(retained.RunnerUUID.String())
	require.NoError(t, err)
	subject, err := hmessaging.RunnerCommandSubject(queueOrg, route)
	require.NoError(t, err)
	first, err := legacy.GetLastMsg(hmessaging.RunnerCommandsStreamName, subject)
	require.NoError(t, err)
	require.Equal(t, retained.Command, first.Data)
	expectedID, err := hmessaging.NativeMessageID(queueOrg, route, "commands", model.PipelineControlCommandType, "pipeline-control:"+receipt.ID.String())
	require.NoError(t, err)
	require.Equal(t, expectedID, first.Header.Get(nats.MsgIdHdr))
	// A recreated slug/current registration and Deployment cleanup cannot
	// replace the captured UUID or rebuild the immutable command on retry.
	deleteControlDeployment(t, f)
	require.NoError(t, f.server.PipelineDispatcher.DispatchControl(f.ctx, zap.NewNop(), delivery))
	replay, err := legacy.GetLastMsg(hmessaging.RunnerCommandsStreamName, subject)
	require.NoError(t, err)
	require.Equal(t, first.Data, replay.Data)
	require.Equal(t, first.Header, replay.Header)
	connection.Close()
	_, retry := hmessaging.AsRetryError(f.server.PipelineDispatcher.DispatchControl(f.ctx, zap.NewNop(), delivery))
	require.True(t, retry, "publication failure retries the existing outbox event")
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	after, err := model.ReadPipelineControl(f.ctx, tx, queueOrg, dep.Id, receipt.ID)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, retained.Command, after.Command)
	require.Empty(t, after.Result, "transport failure never fabricates a refusal")
}

func TestPipelineControlAcceptanceSurvivesClosedNATSAndNormalRelayRecovers(t *testing.T) {
	f, dep, path := nativeControlFixture(t)
	outboxNamespace := uuid.New()
	outboxStart := int64(binary.BigEndian.Uint64(outboxNamespace[:8])&((1<<62)-1)) + 1
	f.exec(`SELECT setval(pg_get_serial_sequence('pending_event_messages', 'id'), $1, false)`, outboxStart)
	brokerURL := os.Getenv("OUTCOME_ACK_TEST_NATS_URL")
	require.NotEmpty(t, brokerURL, "explicit isolated application broker required")
	connection, err := nats.Connect(brokerURL, nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	broker, err := jetstream.New(connection)
	require.NoError(t, err)
	_, err = broker.Stream(f.ctx, hmessaging.EventsStreamName)
	require.NoError(t, err)
	connection.Close()
	f.server.EventPublisher = hnats.NewPublisher(broker, hmessaging.EventsStreamName, zap.NewNop())
	body := map[string]any{"action": "cancel"}
	receipt := controlResponse(t, f.call(http.MethodPost, path, body, f.actor, "closed-events"))
	store := f.store.AsReliableOutboxStore()
	pending, _, err := store.LoadPage(f.ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "failed optimistic publication leaves the committed event durable")
	require.Equal(t, model.PipelineControlOutboxSubject, pending[0].Subject)
	var event map[string]any
	require.NoError(t, json.Unmarshal(pending[0].Payload, &event))
	require.Equal(t, map[string]any{"org_id": queueOrg, "deployment_id": dep.Id.String(), "control_id": receipt.ID.String()}, event)
	replayed := controlResponse(t, f.call(http.MethodPost, path, body, f.actor, "closed-events"))
	require.Equal(t, receipt.ID, replayed.ID)
	afterReplay, _, err := store.LoadPage(f.ctx)
	require.NoError(t, err)
	require.Len(t, afterReplay, 1)
	require.Equal(t, pending[0].Id, afterReplay[0].Id)
	recovered, err := nats.Connect(brokerURL, nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(recovered.Close)
	recoveredBroker, err := jetstream.New(recovered)
	require.NoError(t, err)
	publisher := hnats.NewPublisher(recoveredBroker, hmessaging.EventsStreamName, zap.NewNop())
	_, err = reliableoutbox.FlushNextPage(f.ctx, zap.NewNop(), store, 1, publisher)
	require.NoError(t, err)
	legacy, err := recovered.JetStream()
	require.NoError(t, err)
	delivered, err := legacy.GetLastMsg(hmessaging.EventsStreamName, model.PipelineControlOutboxSubject)
	require.NoError(t, err)
	require.Equal(t, []byte(pending[0].Payload), delivered.Data)
	require.Equal(t, pending[0].Message().ID, delivered.Header.Get(hmessaging.HeaderMessageID))
	empty, _, err := store.LoadPage(f.ctx)
	require.NoError(t, err)
	require.Empty(t, empty)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployment_pipeline_controls`))
	require.Equal(t, 1, f.count(`SELECT count(*) FROM deployments`))
	require.Zero(t, f.count(`SELECT count(*) FROM runner_job_reservations`))
	require.Empty(t, receipt.Result, "transport recovery never fabricates a control result")
}

func TestPipelineControlResultValidatesRetainedScopeAndOnlyUpdatesReceipt(t *testing.T) {
	f, dep, path := nativeControlFixture(t)
	accepted := controlResponse(t, f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "result"))
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	receipt, err := model.ReadPipelineControl(f.ctx, tx, queueOrg, dep.Id, accepted.ID)
	require.NoError(t, err)
	var before string
	require.NoError(
		t,
		tx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'pipeline',to_jsonb(p),'budgets',(SELECT jsonb_agg(b) FROM deployment_dispatch_budgets b),'reservations',(SELECT jsonb_agg(r) FROM runner_job_reservations r))::text FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id WHERE d.id=$1`, dep.Id).
			Scan(&before),
	)
	require.NoError(t, tx.Rollback())
	route, err := hmessaging.NativeRunnerToken(receipt.RunnerUUID.String())
	require.NoError(t, err)
	result := model.PipelineControlResult{Version: model.PipelineControlResultVersion, ControlID: receipt.ID.String(), RolloutID: dep.Id.String(), RequestDigest: receipt.RequestDigest, ExecutionDigest: receipt.ExecutionDigest, Outcome: "applied", Code: "", Phase: "cancelled", Revision: 2}
	payload, err := json.Marshal(result)
	require.NoError(t, err)
	envelope := hmessaging.EventEnvelope{
		ProtocolVersion: hmessaging.ProtocolVersionV1,
		EventID:         "pipeline-control:" + receipt.ID.String() + ":result",
		CommandID:       "pipeline-control:" + receipt.ID.String(),
		OrganizationID:  queueOrg,
		RunnerID:        route,
		DeploymentID:    dep.Id.String(),
		Type:            model.PipelineControlEventType,
		CreatedAt:       time.Now().UTC(),
		Payload:         payload,
	}
	for _, change := range []func(*hmessaging.EventEnvelope){
		func(e *hmessaging.EventEnvelope) { e.OrganizationID = "foreign" },
		func(e *hmessaging.EventEnvelope) { e.RunnerID = queueRunner },
		func(e *hmessaging.EventEnvelope) { e.RunnerID, _ = hmessaging.NativeRunnerToken(uuid.NewString()) },
		func(e *hmessaging.EventEnvelope) { e.DeploymentID = uuid.NewString() },
		func(e *hmessaging.EventEnvelope) { e.EventID += "changed" },
		func(e *hmessaging.EventEnvelope) { e.CommandID += "changed" },
		func(e *hmessaging.EventEnvelope) { e.Type = "control-result" },
	} {
		altered := envelope
		change(&altered)
		raw, err := json.Marshal(altered)
		require.NoError(t, err)
		require.ErrorIs(t, f.server.ApplyPipelineControlResult(f.ctx, raw), ErrRunnerEventTargetInvalid)
	}
	for _, change := range []func(*model.PipelineControlResult){
		func(r *model.PipelineControlResult) { r.ControlID = uuid.NewString() },
		func(r *model.PipelineControlResult) { r.RequestDigest = strings.Repeat("f", 64) },
		func(r *model.PipelineControlResult) { r.ExecutionDigest = strings.Repeat("e", 64) },
		func(r *model.PipelineControlResult) { r.Outcome = "unknown" },
		func(r *model.PipelineControlResult) { r.Code = "unexpected" },
		func(r *model.PipelineControlResult) { r.Revision = 0 },
	} {
		altered := result
		change(&altered)
		envelope.Payload, err = json.Marshal(altered)
		require.NoError(t, err)
		raw, err := json.Marshal(envelope)
		require.NoError(t, err)
		require.ErrorIs(t, f.server.ApplyPipelineControlResult(f.ctx, raw), ErrRunnerEventTargetInvalid)
	}

	envelope.Payload = payload
	validWire, err := json.Marshal(envelope)
	require.NoError(t, err)
	for _, bad := range [][]byte{
		[]byte(strings.Replace(string(validWire), `"type":`, `"type created_at":0,"type":`, 1)),
		[]byte(strings.Replace(string(validWire), `"type":`, `"type\u0020created_at":0,"type":`, 1)),
		[]byte(strings.Replace(string(validWire), `"code":`, `"code phase":"ignored","code":`, 1)),
		[]byte(strings.Replace(string(validWire), `"code":`, `"code\u0020phase":"ignored","code":`, 1)),
		[]byte(strings.Replace(string(validWire), `"code":`, `"code phase":`, 1)),
		[]byte(strings.Replace(string(validWire), `"code":`, `"code\u0020phase":`, 1)),
	} {
		require.ErrorIs(t, f.server.ApplyPipelineControlResult(f.ctx, bad), ErrRunnerEventTargetInvalid)
	}
	envelope.Payload = payload
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	require.NoError(t, f.server.ApplyPipelineControlResult(f.ctx, raw))
	require.NoError(t, f.server.ApplyPipelineControlResult(f.ctx, raw))
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	var after string
	require.NoError(
		t,
		tx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'pipeline',to_jsonb(p),'budgets',(SELECT jsonb_agg(b) FROM deployment_dispatch_budgets b),'reservations',(SELECT jsonb_agg(r) FROM runner_job_reservations r))::text FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id WHERE d.id=$1`, dep.Id).
			Scan(&after),
	)
	require.Equal(t, before, after, "result changes no Deployment, progress, budget, capacity or effective-state owner")
	saved, err := model.ReadPipelineControl(f.ctx, tx, queueOrg, dep.Id, receipt.ID)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, payload, []byte(saved.Result))
	envelope.CreatedAt = envelope.CreatedAt.Add(time.Second)
	conflicting, err := json.Marshal(envelope)
	require.NoError(t, err)
	require.ErrorIs(t, f.server.ApplyPipelineControlResult(f.ctx, conflicting), ErrRunnerEventTargetInvalid, "changed event under deterministic identity refuses")
	deleteControlDeployment(t, f)
	require.NoError(t, f.server.ApplyPipelineControlResult(f.ctx, raw), "retained duplicate stays valid after cleanup")
}

func TestPipelineControlHTTPRejectsAmbiguousBodiesAndOversizeInput(t *testing.T) {
	f, _, path := nativeControlFixture(t)
	for _, body := range []string{
		`{"action":"cancel","action":"approve"}`,
		`{"action":"cancel","Action":"approve"}`,
		`{"action":"cancel","actor_id":"spoof"}`,

		`{"action":"cancel","subject failed_step":"ignored"}`,
		`{"action":"cancel","subject\u0020failed_step":"ignored"}`,
		`{"action subject":"cancel"}`,
		`{"action\u0020subject":"cancel"}`,
		`{"action":"cancel","subject":null}`,
		`{"action":"cancel"} {}`,
		`{"action":"approve"}`,
		`{"action":"repair","failed_step":"post","revision":0}`,
	} {
		request := f.request(http.MethodPost, path, nil, f.actor, "invalid")
		request.Header.Set("Content-Type", "application/json")
		request.Body = io.NopCloser(strings.NewReader(body))
		request.ContentLength = int64(len(body))
		response := f.serve(request)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	}
	request := f.request(http.MethodPost, path, nil, f.actor, "oversize")
	request.Header.Set("Content-Type", "application/json")
	body := `{"action":"cancel","subject":"` + strings.Repeat("x", consoleJSONLimit) + `"}`
	request.Body = io.NopCloser(strings.NewReader(body))
	request.ContentLength = int64(len(body))
	require.Equal(t, http.StatusRequestEntityTooLarge, f.serve(request).Code)
	require.Zero(t, f.count(`SELECT count(*) FROM deployment_pipeline_controls`))
	require.Zero(t, f.count(`SELECT count(*) FROM pending_event_messages WHERE routing_key='io.platform-orchestrator.pipeline.control.requested'`))
}

func TestPipelineControlHTTPBindsCapturedExecutionAuthority(t *testing.T) {
	for _, test := range []struct {
		action     string
		subject    string
		failedStep string
		revision   int64
	}{
		{action: "approve", subject: strings.Repeat("a", 64)},
		{action: "deny", subject: strings.Repeat("b", 64)},
		{action: "cancel"},
		{action: "repair", failedStep: "bootstrap-lost-ack", revision: 7},
	} {
		t.Run(test.action, func(t *testing.T) {
			f, dep, path := nativeControlFixture(t)
			body := map[string]any{"action": test.action}
			if test.subject != "" {
				body["subject"] = test.subject
			}
			if test.failedStep != "" {
				body["failed_step"] = test.failedStep
				body["revision"] = test.revision
			}
			accepted := controlResponse(t, f.call(http.MethodPost, path, body, f.actor, "authority-"+test.action))
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			retained, err := model.ReadPipelineControl(f.ctx, tx, queueOrg, dep.Id, accepted.ID)
			require.NoError(t, err)
			var execution []byte
			require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT command FROM deployment_pipeline_runs WHERE deployment_id=$1`, dep.Id).Scan(&execution))
			require.NoError(t, tx.Rollback())
			var original, control hmessaging.CommandEnvelope
			require.NoError(t, json.Unmarshal(execution, &original))
			require.NoError(t, json.Unmarshal(retained.Command, &control))
			var originalSigned, controlSigned struct {
				Payload   json.RawMessage `json:"payload"`
				Signature string          `json:"signature"`
			}
			require.NoError(t, json.Unmarshal(original.Payload, &originalSigned))
			require.NoError(t, json.Unmarshal(control.Payload, &controlSigned))
			var originalTimes struct {
				IssuedAt  time.Time `json:"issued_at"`
				ExpiresAt time.Time `json:"expires_at"`
			}
			require.NoError(t, json.Unmarshal(originalSigned.Payload, &originalTimes))
			var request model.PipelineControlRequest
			require.NoError(t, json.Unmarshal(controlSigned.Payload, &request))
			require.Equal(t, test.action, request.Action)
			require.Equal(t, test.subject, request.Subject)
			require.Equal(t, test.failedStep, request.FailedStep)
			require.Equal(t, test.revision, request.Revision)
			signature, err := base64.StdEncoding.DecodeString(controlSigned.Signature)
			require.NoError(t, err)
			require.True(t, ed25519.Verify(f.server.PipelineDispatcher.Key.Public().(ed25519.PublicKey), controlSigned.Payload, signature))
			require.Equal(t, *dep.NativeTarget, request.Target)
			require.Equal(t, pipelineControlDigest(originalSigned.Payload), request.ExecutionDigest)
			require.Equal(t, f.actor.String(), request.ActorID)
			require.Equal(t, queueOrg, request.OrgID)
			require.Equal(t, dep.DeploymentEnvUuid.String(), request.EnvironmentID)
			if test.action == "repair" {
				require.Equal(t, pipelineCommandTTL, request.ExpiresAt.Sub(request.IssuedAt))
			} else {
				require.Equal(t, originalTimes.ExpiresAt, request.ExpiresAt)
			}
			require.False(t, request.IssuedAt.Before(originalTimes.IssuedAt))
			require.True(t, request.IssuedAt.Before(request.ExpiresAt))
			require.Equal(t, request.IssuedAt, control.CreatedAt)
			require.Equal(t, request.ExpiresAt, control.ExpiresAt)
			require.LessOrEqual(t, len(retained.Command), model.PipelineControlMaxBytes)
		})
	}
}

func TestPipelineControlResultResolvedGetSchemas(t *testing.T) {
	for _, test := range []struct {
		outcome string
		code    string
		phase   string
	}{
		{outcome: "applied", phase: "cancelled"},
		{outcome: "refused", code: "control_scope_refused", phase: "accepted"},
	} {
		t.Run(test.outcome, func(t *testing.T) {
			f, dep, path := nativeControlFixture(t)
			accepted := controlResponse(t, f.call(http.MethodPost, path, map[string]any{"action": "cancel"}, f.actor, "resolved-"+test.outcome))
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			receipt, err := model.ReadPipelineControl(f.ctx, tx, queueOrg, dep.Id, accepted.ID)
			require.NoError(t, err)
			var before string
			require.NoError(
				t,
				tx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'pipeline',to_jsonb(p),'budgets',(SELECT jsonb_agg(b) FROM deployment_dispatch_budgets b),'reservations',(SELECT jsonb_agg(r) FROM runner_job_reservations r))::text FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id WHERE d.id=$1`, dep.Id).
					Scan(&before),
			)
			require.NoError(t, tx.Rollback())
			route, err := hmessaging.NativeRunnerToken(receipt.RunnerUUID.String())
			require.NoError(t, err)
			result := model.PipelineControlResult{
				Version:         model.PipelineControlResultVersion,
				ControlID:       receipt.ID.String(),
				RolloutID:       dep.Id.String(),
				RequestDigest:   receipt.RequestDigest,
				ExecutionDigest: receipt.ExecutionDigest,
				Outcome:         test.outcome,
				Code:            test.code,
				Phase:           test.phase,
				Revision:        2,
			}
			payload, err := json.Marshal(result)
			require.NoError(t, err)
			envelope := hmessaging.EventEnvelope{
				ProtocolVersion: hmessaging.ProtocolVersionV1,
				EventID:         "pipeline-control:" + receipt.ID.String() + ":result",
				CommandID:       "pipeline-control:" + receipt.ID.String(),
				OrganizationID:  queueOrg,
				RunnerID:        route,
				DeploymentID:    dep.Id.String(),
				Type:            model.PipelineControlEventType,
				CreatedAt:       time.Now().UTC(),
				Payload:         payload,
			}
			raw, err := json.Marshal(envelope)
			require.NoError(t, err)
			require.NoError(t, f.server.ApplyPipelineControlResult(f.ctx, raw))
			response := f.call(http.MethodGet, path+"/"+receipt.ID.String(), nil, f.actor, "")
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			requirePipelineSchema(t, "DeploymentPipelineControlReceipt", response.Body.Bytes())
			var view DeploymentPipelineControlReceipt
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &view))
			require.Equal(t, DeploymentPipelineControlReceiptStatusResolved, view.Status)
			require.NotNil(t, view.Result)
			actual, err := json.Marshal(view.Result)
			require.NoError(t, err)
			require.JSONEq(t, string(payload), string(actual))
			tx, err = f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			var after string
			require.NoError(
				t,
				tx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'pipeline',to_jsonb(p),'budgets',(SELECT jsonb_agg(b) FROM deployment_dispatch_budgets b),'reservations',(SELECT jsonb_agg(r) FROM runner_job_reservations r))::text FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id WHERE d.id=$1`, dep.Id).
					Scan(&after),
			)
			require.NoError(t, tx.Rollback())
			require.Equal(t, before, after, "result acceptance and GET change no Deployment, progress, budget or capacity")
		})
	}
}
