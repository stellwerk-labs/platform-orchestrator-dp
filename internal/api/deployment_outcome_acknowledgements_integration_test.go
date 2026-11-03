//go:build integration

package api

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hnats"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hmessaging"
	iamapi "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genevents"
)

func TestOutcomeAcknowledgementRequiresPermissionAndReleasesCapturedBaseRecoveryOnce(t *testing.T) {
	f := newLateResultFixture(t)
	failed := f.fail(t)
	actor := userid.NewHumanUserId()
	var allowed atomic.Bool
	iam := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body iamapi.InternalAuthorizeBody
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		assert.Equal(t, actor, body.UserId)
		if assert.Len(t, body.Checks, 1) {
			assert.Equal(t, PermissionDeploymentOutcomeAcknowledge, body.Checks[0].Permission)
			assert.Contains(t, []string{"env:" + f.params.DeploymentEnvUuid.String(), "organization:late-org"}, body.Checks[0].Resource)
		}
		if allowed.Load() {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"HTTP-403","message":"not permitted","details":{}}`))
	}))
	t.Cleanup(iam.Close)
	iamClient, err := iamapi.NewClientWithResponses(iam.URL)
	require.NoError(t, err)
	f.server.IamClient = iamClient
	e, err := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{AppName: "outcome-acknowledgement-test", Logger: zap.NewNop(), OpenAPIRawSchema: MustDecodeOpenApiSpec(), OpenAPISkipperFn: SkipGeneratedOpenAPIValidation})
	require.NoError(t, err)
	e.Use(middleware.EchoCtxMiddleware)
	f.server.MapRoutes(e)
	acknowledge := func(from, reason string) *httptest.ResponseRecorder {
		body, err := json.Marshal(DeploymentOutcomeAcknowledgementBody{Reason: reason})
		require.NoError(t, err)
		request := httptest.NewRequest(http.MethodPost, "/internal/orgs/late-org/deployments/"+f.forward.Id.String()+"/actions/acknowledge-outcome", strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("From", from)
		response := httptest.NewRecorder()
		e.ServeHTTP(response, request)
		return response
	}
	const reason = "runner job confirmed deleted"

	require.Equal(t, http.StatusForbidden, acknowledge(actor.String(), reason).Code, "explicit permission is required")
	allowed.Store(true)
	open := acknowledge(actor.String(), reason)
	require.Equal(t, http.StatusConflict, open.Code)
	require.Contains(t, open.Body.String(), "dispatch window is open")
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours', deadline=now()-interval '1 hour' WHERE deployment_id=$1`, f.forward.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Separate test schemas share the isolated NATS stream, so reserve a unique
	// outbox ID range rather than colliding with JetStream's real deduplication.
	outboxNamespace := uuid.New()
	outboxStart := int64(binary.BigEndian.Uint64(outboxNamespace[:8])&((1<<62)-1)) + 1
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `SELECT setval(pg_get_serial_sequence('pending_event_messages', 'id'), $1, false)`, outboxStart)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// A real outbox insertion failure must roll back both the audit and ownership
	// release. A later ordinary request can then record the acknowledgement once.
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `CREATE FUNCTION reject_ack_cleanup_wake() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture rejects cleanup wake'; END $$;
CREATE TRIGGER reject_ack_cleanup_wake BEFORE INSERT ON pending_event_messages FOR EACH ROW EXECUTE FUNCTION reject_ack_cleanup_wake()`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	refused := acknowledge(actor.String(), reason)
	require.Equal(t, http.StatusInternalServerError, refused.Code)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	var auditCount int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_outcome_acknowledgements WHERE deployment_id=$1`, f.forward.Id).Scan(&auditCount))
	require.Zero(t, auditCount, "failed wake insertion cannot commit an acknowledgement")
	unreconciled, err := model.DeploymentOutcomeUnreconciled(f.ctx, tx, f.forward.Id)
	require.NoError(t, err)
	require.True(t, unreconciled, "failed wake insertion cannot release unknown Runner ownership")
	_, err = tx.ExecContext(f.ctx, `DROP TRIGGER reject_ack_cleanup_wake ON pending_event_messages; DROP FUNCTION reject_ack_cleanup_wake()`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Close an actual connected NATS transport. The audit must still commit,
	// leaving exactly one durable outbox message for the ordinary relay.
	natsURL := os.Getenv("OUTCOME_ACK_TEST_NATS_URL")
	require.NotEmpty(t, natsURL, "an explicit isolated NATS fixture is required")
	connection, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	require.NoError(t, err)
	js, err := jetstream.New(connection)
	require.NoError(t, err)
	_, err = js.Stream(f.ctx, hmessaging.EventsStreamName)
	require.NoError(t, err, "the isolated application NATS fixture must already own its standard events stream")
	connection.Close()
	f.server.EventPublisher = hnats.NewPublisher(js, hmessaging.EventsStreamName, zap.NewNop())
	created := acknowledge(actor.String(), reason)
	require.Equal(t, http.StatusCreated, created.Code, created.Body.String())
	var ack DeploymentOutcomeAcknowledgement
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &ack))
	require.False(t, ack.Replayed)
	require.Equal(t, actor, ack.ActorId)
	require.Equal(t, reason, ack.Reason)
	require.Equal(t, f.forward.Id, ack.DeploymentId)
	require.Equal(t, f.params.DeploymentEnvUuid, ack.EnvironmentId)
	require.Equal(t, "dev.example.rollouts", ack.PluginId)

	store := f.store.AsReliableOutboxStore()
	pending, _, err := store.LoadPage(f.ctx)
	require.NoError(t, err)
	require.Len(t, pending, 1, "disconnected transport leaves the committed cleanup wake durable")
	require.Equal(t, string(genevents.IoPlatformOrchestratorDeploymentUpdated), pending[0].Subject)
	var wake events.CloudEvent[genevents.DeploymentChangedData]
	require.NoError(t, json.Unmarshal(pending[0].Payload, &wake))
	require.Equal(t, genevents.IoPlatformOrchestratorDeploymentUpdated, wake.Type)
	require.Equal(t, f.params.DeploymentEnvUuid, wake.Data.EnvUuid)

	replayed := acknowledge(actor.String(), reason)
	require.Equal(t, http.StatusOK, replayed.Code)
	var replay DeploymentOutcomeAcknowledgement
	require.NoError(t, json.Unmarshal(replayed.Body.Bytes(), &replay))
	require.True(t, replay.Replayed)
	require.True(t, ack.AcknowledgedAt.Equal(replay.AcknowledgedAt))
	afterReplay, _, err := store.LoadPage(f.ctx)
	require.NoError(t, err)
	require.Len(t, afterReplay, 1, "replay cannot enqueue another cleanup wake")
	require.Equal(t, pending[0].Id, afterReplay[0].Id)

	recovered, err := nats.Connect(natsURL, nats.Timeout(3*time.Second))
	require.NoError(t, err)
	t.Cleanup(recovered.Close)
	recoveredJS, err := jetstream.New(recovered)
	require.NoError(t, err)
	stream, err := recoveredJS.Stream(f.ctx, hmessaging.EventsStreamName)
	require.NoError(t, err)
	consumerName := "outcome_ack_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	consumer, err := stream.CreateConsumer(f.ctx, jetstream.ConsumerConfig{
		Durable: consumerName, FilterSubject: pending[0].Subject,
		DeliverPolicy: jetstream.DeliverNewPolicy, AckPolicy: jetstream.AckExplicitPolicy,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.DeleteConsumer(context.WithoutCancel(f.ctx), consumerName)) })
	recoveredPublisher := hnats.NewPublisher(recoveredJS, hmessaging.EventsStreamName, zap.NewNop())
	_, err = reliableoutbox.FlushNextPage(f.ctx, zap.NewNop(), store, 1, recoveredPublisher)
	require.NoError(t, err)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		message, err := consumer.Next(jetstream.FetchMaxWait(100 * time.Millisecond))
		if !assert.NoError(c, err) {
			return
		}
		var delivered events.CloudEvent[genevents.DeploymentChangedData]
		if !assert.NoError(c, json.Unmarshal(message.Data(), &delivered)) {
			return
		}
		assert.Equal(c, ack.DeploymentId, delivered.Data.DeploymentId)
		assert.Equal(c, ack.EnvironmentId, delivered.Data.EnvUuid)
		assert.Equal(c, pending[0].Message().ID, message.Headers().Get(hmessaging.HeaderMessageID))
		assert.NoError(c, message.Ack())
	}, 5*time.Second, 10*time.Millisecond, "the normal outbox relay must deliver the actual cleanup wake")
	empty, _, err := store.LoadPage(f.ctx)
	require.NoError(t, err)
	require.Empty(t, empty, "only successful JetStream publication completes the outbox row")
	require.Equal(t, http.StatusConflict, acknowledge(actor.String(), "a different story").Code)
	require.Equal(t, http.StatusForbidden, acknowledge(userid.InternalSystemUuid.String(), reason).Code)

	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var audits int
	var auditedActor, auditedReason string
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*), min(actor_id::text), min(reason) FROM deployment_outcome_acknowledgements`).Scan(&audits, &auditedActor, &auditedReason))
	require.Equal(t, 1, audits)
	require.Equal(t, actor.String(), auditedActor)
	require.Equal(t, reason, auditedReason)
	got, _, _, _, err := f.store.GetDeployment(f.ctx, tx, "late-org", f.forward.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, failed.Status, got.Status)
	require.Equal(t, failed.Revision, got.Revision, "acknowledgement never rewrites the Deployment")

	adapter := ExternalDeploymentHTTPAdapter{Admission: ExternalDeploymentAdmissionService{Database: f.store}}
	receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(f.ctx, tx, "late-org", "dev.example.rollouts", f.forward.Id)
	require.NoError(t, err)
	projected, err := adapter.projectExternalDeploymentStatus(f.ctx, tx, *got, *receipt, f.project, f.params.DeploymentEnvUuid)
	require.NoError(t, err, "the plugin can now observe the acknowledged failure")
	require.Equal(t, "failed", projected.Status)
	recovery := f.params
	recovery.Mode = model.DeploymentModeRollback
	recovery.RollbackToId = opt.Of(f.base.Id)
	recovery.ExpectedCurrentDeploymentID = opt.Of(f.forward.Id)
	rollback, err := f.store.CreateDeployment(f.ctx, tx, "late-org", "p", "e", recovery)
	require.NoError(t, err, "captured-base recovery accepts the audited acknowledgement as the stop attestation")
	require.Equal(t, f.base.Id, rollback.RollbackToId.Must())
}
