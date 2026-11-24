//go:build integration

package worker

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRunnerRefusalsConsumerRecoversAfterTenDatabaseFailures(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	dsn, address := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"), os.Getenv("OUTCOME_ACK_TEST_NATS_URL")
	require.NotEmpty(t, dsn)
	require.NotEmpty(t, address)
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "refusal_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	store, err := model.NewDatabaser(ctx, zap.NewNop(), dsn+" search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	dep, command, route := retainedRefusalCommand(t, ctx, store)
	before := refusalDomain(t, ctx, store, dep.Id)
	connection, err := nats.Connect(address, nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	require.NoError(t, err)
	require.NoError(t, hnats.EnsureStandardStreams(ctx, js, 1))
	var active atomic.Pointer[api.Server]
	active.Store(&api.Server{Database: store})
	var attempts atomic.Int32
	apply := func(ctx context.Context, delivery hmessaging.Delivery) error {
		attempts.Add(1)
		return active.Load().ApplyNativeCommandRefusal(ctx, delivery)
	}
	w := &Worker{JetStream: js, Logger: zap.NewNop()}
	configured, err := w.BuildRunnerRefusalsConsumer(ctx, apply)
	require.NoError(t, err)
	require.NoError(t, configured.Close(ctx))
	raw, err := js.Consumer(ctx, hmessaging.DeadLettersStreamName, runnerRefusalsConsumerDurable)
	require.NoError(t, err)
	// Keep production durable configuration, shorten only the existing library's test backoff.
	consumer, err := hnats.NewConsumer(raw, apply, hnats.ProcessingConfig{MaxDeliveries: 10, RetryBackOff: []time.Duration{5 * time.Millisecond}})
	require.NoError(t, err)
	var replacement model.Databaser
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case runErr := <-done:
			if replacement != nil {
				require.NoError(t, replacement.Close())
			}
			if runErr != nil {
				require.ErrorIs(t, runErr, context.Canceled)
			}
		case <-time.After(5 * time.Second):
			t.Error("refusal consumer did not stop")
		}
	})
	require.NoError(t, store.Close(), "this pool is deliberately closed, not reopened later")
	subject, err := hmessaging.RunnerCommandSubject(dep.OrgId, route)
	require.NoError(t, err)
	dlq, err := hmessaging.DeadLetterSubject(subject)
	require.NoError(t, err)
	publisher := hnats.NewPublisher(js, hmessaging.DeadLettersStreamName, zap.NewNop())
	require.NoError(t, publisher.Publish(ctx, hmessaging.Message{ID: uuid.NewString(), Subject: dlq, Data: command, Header: hmessaging.Header{"Po-Dead-Letter-Reason": {"capability_refused"}}}))
	require.Eventually(t, func() bool { return attempts.Load() > 10 }, 5*time.Second, 5*time.Millisecond)
	pending, err := raw.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, -1, pending.Config.MaxDeliver)
	require.Equal(t, 1, pending.NumAckPending)
	letters, err := js.Stream(ctx, hmessaging.DeadLettersStreamName)
	require.NoError(t, err)
	retainedBeforeRecovery, err := letters.Info(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, retainedBeforeRecovery.State.Msgs, "nil DLQ publisher never recursively republishes failures")
	replacement, err = model.NewDatabaser(ctx, zap.NewNop(), dsn+" search_path="+schema)
	require.NoError(t, err)
	active.Store(&api.Server{Database: replacement})
	require.Eventually(t, func() bool {
		info, err := raw.Info(ctx)
		return err == nil && info.NumAckPending == 0 && info.NumPending == 0
	}, 5*time.Second, 10*time.Millisecond)
	read, err := replacement.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = read.Rollback() }()
	var diagnostic, completion int
	require.NoError(t, read.QueryRowContext(ctx, `SELECT count(*) FILTER(WHERE event_type='command-delivery-refused'),count(*) FILTER(WHERE event_type='deployment-result') FROM runner_event_inbox WHERE deployment_id=$1`, dep.Id).Scan(&diagnostic, &completion))
	require.Equal(t, 1, diagnostic)
	require.Zero(t, completion)
	stored, _, _, _, err := replacement.GetDeployment(ctx, read, dep.OrgId, dep.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentStatusExecuting, stored.Status)
	require.False(t, stored.CompletedAt.IsSet())
	require.Equal(t, dep.Revision, stored.Revision)
	require.Equal(t, before, refusalDomain(t, ctx, replacement, dep.Id))
	retained, err := js.Stream(ctx, hmessaging.DeadLettersStreamName)
	require.NoError(t, err)
	message, err := retained.GetLastMsgForSubject(ctx, dlq)
	require.NoError(t, err)
	require.Equal(t, command, message.Data, "consumer ACK preserves retained operator history")
}

// Run separately against an empty invocation-owned broker, never a populated service broker.
func TestRunnerRefusalsConsumerRefusesDestructiveRetention(t *testing.T) {
	connection, err := nats.Connect(os.Getenv("OUTCOME_ACK_TEST_NATS_URL"), nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	require.NoError(t, err)
	_, err = js.Stream(t.Context(), hmessaging.DeadLettersStreamName)
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound, "requires the separately owned empty-broker invocation")
	stream, err := js.CreateStream(t.Context(), jetstream.StreamConfig{Name: hmessaging.DeadLettersStreamName, Subjects: []string{"po.dlq.>"}, Retention: jetstream.WorkQueuePolicy})
	require.NoError(t, err)
	legacy, err := js.Publish(t.Context(), "po.dlq.legacy.history", []byte("retained operator history"))
	require.NoError(t, err)
	w := &Worker{JetStream: js, Logger: zap.NewNop()}
	consumer, err := w.BuildRunnerRefusalsConsumer(t.Context(), func(context.Context, hmessaging.Delivery) error { return nil })
	require.ErrorContains(t, err, "retained dead-letter history")
	require.Nil(t, consumer)
	message, err := stream.GetMsg(t.Context(), legacy.Sequence)
	require.NoError(t, err)
	require.Equal(t, []byte("retained operator history"), message.Data)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.Empty(t, info.State.Consumers, "unsupported retention creates no consuming durable")
}

// Accepted-state fixture, not Core admission or physical Runner execution proof.
func retainedRefusalCommand(t *testing.T, ctx context.Context, store model.Databaser) (*model.DeploymentSummary, []byte, string) {
	t.Helper()
	org, environment := "refusal-"+uuid.NewString(), uuid.New()
	target := &cp.NativeExecutionTargetReference{RunnerId: "site", RunnerUuid: uuid.New(), RuntimeVersion: cp.NativeTargetRuntimeV2, PipelineVersion: cp.NativeTargetPipelineV2,
		StateBackendType: "kubernetes", StateConfigurationDigest: strings.Repeat("b", 64), ToolsDigest: strings.Repeat("a", 64)}
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dep, err := store.CreateDeployment(ctx, tx, org, "project", "env", model.CreateDeploymentParams{NativeTarget: target, DeploymentEnvUuid: environment, CreatedBy: uuid.New(), Mode: model.DeploymentModeDeploy,
		RunnerId: "site", RunnerLogLevel: "info", Pipeline: definition, PipelineBackend: "kubernetes", Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`), Graph: model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), Tofu: model.RawTofu("terraform {}")})
	require.NoError(t, err)
	now := time.Now().UTC()
	payload, err := json.Marshal(map[string]any{"version": target.PipelineVersion, "native_target": target, "id": dep.Id.String(), "org_id": org, "runner_id": target.RunnerId, "runner_uuid": target.RunnerUuid.String(),
		"environment_id": environment.String(), "tools_digest": target.ToolsDigest, "issued_at": now, "expires_at": now.Add(time.Hour),
		"execution_limits": model.PipelineExecutionLimits{PlanSeconds: 1800, ApplySeconds: 7200, TaskSeconds: 1800},
		"request":          map[string]any{"id": dep.Id.String(), "org_id": org, "runner_id": target.RunnerId, "runner_uuid": target.RunnerUuid.String(), "environment_id": environment.String(), "mode": "deploy", "backend": "kubernetes", "tool": "opentofu", "encrypted_outputs_recipient": ""}})
	require.NoError(t, err)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signed, err := json.Marshal(map[string]any{"payload": json.RawMessage(payload), "signature": base64.StdEncoding.EncodeToString(ed25519.Sign(key, payload))})
	require.NoError(t, err)
	route, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	require.NoError(t, err)
	command, err := json.Marshal(
		hmessaging.CommandEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, Type: "execute-rollout-v1", CommandID: "pipeline:" + dep.Id.String(), OrganizationID: org, RunnerID: route, DeploymentID: dep.Id.String(), CreatedAt: now, ExpiresAt: now.Add(time.Hour), Payload: signed},
	)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET command=$2,no_effect_blocked=true WHERE deployment_id=$1`, dep.Id, command)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now(),deadline=now()+interval '1 hour' WHERE deployment_id=$1`, dep.Id)
	require.NoError(t, err)
	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO deployment_admission_receipts(org_id,admission_id,project_uuid,environment_uuid,actor_id,request_digest,mode,module_versions,carry_forward_module_versions,confirmed_restricted_version_uuids,core_epoch,disposition,deployment_id,draft_test_snapshots,native_target)
 SELECT $2,$3,$4,d.de_id,d.created_by,repeat('a',64),d.mode,'{}','{}','[]',1,'committed',d.id,'{}',d.native_target FROM deployments d WHERE d.id=$1`,
		dep.Id,
		org,
		uuid.New(),
		uuid.New(),
	)
	require.NoError(t, err)
	require.NoError(t, model.ReserveRunnerJob(ctx, tx, dep, 2))
	require.NoError(t, tx.Commit())
	return dep, command, route
}

func refusalDomain(t *testing.T, ctx context.Context, store model.Databaser, id uuid.UUID) string {
	t.Helper()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var snapshot string
	require.NoError(t, tx.QueryRowContext(ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d)-'status_message','environment',to_jsonb(e),
 'pipeline',(SELECT to_jsonb(p) FROM deployment_pipeline_runs p WHERE p.deployment_id=d.id),'budget',(SELECT to_jsonb(b) FROM deployment_dispatch_budgets b WHERE b.deployment_id=d.id),
 'reservation',(SELECT to_jsonb(r) FROM runner_job_reservations r WHERE r.deployment_id=d.id),'lease',(SELECT to_jsonb(l) FROM environment_deployment_leases l WHERE l.deployment_id=d.id),
 'admission',(SELECT to_jsonb(a) FROM deployment_admission_receipts a WHERE a.deployment_id=d.id),'effective',(SELECT to_jsonb(s) FROM deployment_effective_state s WHERE s.environment_id=e.id),
 'outbox',(SELECT count(*) FROM pending_event_messages))::text FROM deployments d JOIN deployment_environments e ON e.id=d.de_id WHERE d.id=$1`, id).Scan(&snapshot))
	return snapshot
}
