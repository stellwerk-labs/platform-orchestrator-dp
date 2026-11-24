//go:build integration

package api_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	vaultapi "github.com/hashicorp/vault/api"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers/runnerresulthandler"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Real normal-consumer routing and retained authority, not Core admission, IAM
// authorization or Runner execution. The existing API tests cover scoped reads.
func TestNativeMixedConsumerRetainsControlsAndPhaseLogs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	dsn, address := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING"), os.Getenv("OUTCOME_ACK_TEST_NATS_URL")
	require.NotEmpty(t, dsn)
	require.NotEmpty(t, address)
	require.NotEmpty(t, os.Getenv("COST_POLICY_TEST_VAULT_ADDR"))
	admin, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "mixed_native_" + strings.ReplaceAll(uuid.NewString(), "-", "")
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
	connection, err := nats.Connect(address, nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	js, err := jetstream.New(connection)
	require.NoError(t, err)
	require.NoError(t, hnats.EnsureStandardStreams(ctx, js, 1))
	vc := vaultapi.DefaultConfig()
	vc.Address = os.Getenv("COST_POLICY_TEST_VAULT_ADDR")
	client, err := vaultapi.NewClient(vc)
	require.NoError(t, err)
	client.SetToken(os.Getenv("COST_POLICY_TEST_VAULT_TOKEN"))
	server := &api.Server{Database: store, Logger: zap.NewNop(), Vault: vault.NewVaultClient(client, zap.NewNop())}
	material, err := (api.DeploymentLogMaterialService{Vault: server.Vault}).Generate(ctx)
	require.NoError(t, err)
	org, environment := "mixed-"+uuid.NewString(), uuid.New()
	target := &cp.NativeExecutionTargetReference{
		RunnerId: "site", RunnerUuid: uuid.New(), RuntimeVersion: cp.NativeTargetRuntimeV2,
		PipelineVersion: cp.NativeTargetPipelineV2, StateBackendType: "kubernetes",
		StateConfigurationDigest: strings.Repeat("b", 64), ToolsDigest: strings.Repeat("a", 64),
	}
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dep, err := store.CreateDeployment(ctx, tx, org, "project", "environment", model.CreateDeploymentParams{
		NativeTarget: target, DeploymentEnvUuid: environment, CreatedBy: uuid.New(), Mode: model.DeploymentModeDeploy,
		RunnerId: "site", RunnerLogLevel: "info", Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`),
		Graph: model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), Tofu: model.RawTofu("terraform {}"),
		Pipeline: definition, PipelineBackend: "kubernetes", EncryptedLogsRecipient: opt.Of(material.Recipient),
	})
	require.NoError(t, err)
	require.NoError(t, model.StoreDeploymentLogMaterial(ctx, tx, org, dep.Id, *material))
	route, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	require.NoError(t, err)
	now := time.Now().UTC()
	payload, err := json.Marshal(map[string]any{
		"version": target.PipelineVersion, "native_target": target, "id": dep.Id.String(), "org_id": org,
		"runner_id": target.RunnerId, "runner_uuid": target.RunnerUuid.String(), "environment_id": environment.String(),
		"tools_digest": target.ToolsDigest, "issued_at": now, "expires_at": now.Add(time.Hour),
		"execution_limits": model.PipelineExecutionLimits{PlanSeconds: 1800, ApplySeconds: 7200, TaskSeconds: 1800},
		"request": map[string]any{"id": dep.Id.String(), "org_id": org, "runner_id": target.RunnerId,
			"runner_uuid": target.RunnerUuid.String(), "environment_id": environment.String(), "backend": "kubernetes",
			"tool": "opentofu", "mode": "deploy", "encrypted_logs_recipient": material.Recipient},
	})
	require.NoError(t, err)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signed, err := json.Marshal(map[string]any{"payload": json.RawMessage(payload), "signature": base64.StdEncoding.EncodeToString(ed25519.Sign(key, payload))})
	require.NoError(t, err)
	command, err := json.Marshal(hmessaging.CommandEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1,
		CommandID: "pipeline:" + dep.Id.String(), OrganizationID: org, RunnerID: route, DeploymentID: dep.Id.String(),
		Type: "execute-rollout-v1", CreatedAt: now, ExpiresAt: now.Add(time.Hour), Payload: signed})
	require.NoError(t, err)
	_, err = model.FrozenPipelinePayload(command, dep)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, dep.Id, command)
	require.NoError(t, err)
	receipt := &model.PipelineControl{ID: uuid.New(), OrgID: org, DeploymentID: dep.Id, EnvironmentID: environment,
		RunnerUUID: target.RunnerUuid, ActorID: uuid.New(), ClientKey: "mixed-consumer", IntentDigest: nativelogs.Hash([]byte(`{"action":"cancel"}`)),
		ExecutionDigest: nativelogs.Hash(payload), Action: "cancel", CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	controlPayload, err := json.Marshal(model.PipelineControlRequest{Version: model.PipelineControlVersion, ID: receipt.ID.String(),
		RolloutID: dep.Id.String(), OrgID: org, EnvironmentID: environment.String(), Target: *target,
		ExecutionDigest: receipt.ExecutionDigest, ActorID: receipt.ActorID.String(), Action: "cancel", IssuedAt: now, ExpiresAt: receipt.ExpiresAt})
	require.NoError(t, err)
	controlSigned, err := json.Marshal(map[string]any{"payload": json.RawMessage(controlPayload), "signature": base64.StdEncoding.EncodeToString(ed25519.Sign(key, controlPayload))})
	require.NoError(t, err)
	receipt.RequestDigest = nativelogs.Hash(controlPayload)
	receipt.Command, err = json.Marshal(hmessaging.CommandEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1,
		CommandID: "pipeline-control:" + receipt.ID.String(), OrganizationID: org, RunnerID: route, DeploymentID: dep.Id.String(),
		Type: model.PipelineControlCommandType, CreatedAt: now, ExpiresAt: receipt.ExpiresAt, Payload: controlSigned})
	require.NoError(t, err)
	require.NoError(t, model.InsertPipelineControl(ctx, tx, receipt))
	require.NoError(t, tx.Commit())
	state := func() string {
		read, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = read.Rollback() }()
		var value string
		require.NoError(
			t,
			read.QueryRowContext(ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'progress',p.progress-'log_references'-'log_references_limited','command',encode(p.command,'hex'),'environments',(SELECT jsonb_agg(e) FROM deployment_environments e),'budgets',(SELECT jsonb_agg(b) FROM deployment_dispatch_budgets b),'reservations',(SELECT jsonb_agg(r) FROM runner_job_reservations r))::text FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id WHERE d.id=$1`, dep.Id).
				Scan(&value),
		)
		return value
	}
	before := state()
	objects, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: nativelogs.Bucket, Storage: jetstream.MemoryStorage})
	require.NoError(t, err)
	server.RunnerLogsReader = func(ctx context.Context, object string) (io.ReadCloser, error) { return objects.Get(ctx, object) }
	authority := nativelogs.Authority{OrganizationID: org, EnvironmentID: environment.String(), DeploymentID: dep.Id.String(),
		CommandDigest: nativelogs.Hash(payload), Target: nativelogs.Target{RunnerID: target.RunnerId, RunnerUUID: target.RunnerUuid.String(),
			RuntimeVersion: string(target.RuntimeVersion), PipelineVersion: string(target.PipelineVersion), StateBackendType: "kubernetes",
			StateConfigurationDigest: target.StateConfigurationDigest, ToolsDigest: target.ToolsDigest},
		Phase: "apply", ExecutionID: dep.Id.String(), CaptureRevision: 2}
	recipient, err := age.ParseX25519Recipient(material.Recipient)
	require.NoError(t, err)
	ciphertext, err := nativelogs.Encode(nativelogs.Envelope{Version: 1, Authority: authority, Status: nativelogs.PendingTransfer, Logs: []byte("mixed private canary\n")}, recipient)
	require.NoError(t, err)
	descriptor := nativelogs.Descriptor{Version: 1, Authority: authority, Status: nativelogs.PendingTransfer,
		CiphertextSHA256: nativelogs.Hash(ciphertext), CiphertextBytes: len(ciphertext), ObjectKey: authority.ObjectKey(nativelogs.Hash(ciphertext))}
	_, err = objects.Put(ctx, jetstream.ObjectMeta{Name: descriptor.ObjectKey}, bytes.NewReader(ciphertext))
	require.NoError(t, err)
	legacy, err := hnats.EnsureDurableConsumer(ctx, js, hnats.DurableConsumerConfig{
		Stream: hmessaging.RunnerEventsStreamName, Durable: "platform-orchestrator-dp-runner-results",
		FilterSubjects: []string{runnerresulthandler.DeploymentResultSubjects, runnerresulthandler.RunnerErrorSubjects, runnerresulthandler.PipelineProgressSubjects},
		MaxDeliver:     worker.MainConsumerMaxDeliveries, AckWait: worker.MainConsumerAckWait, MaxAckPending: worker.MainConsumerConcurrency,
	})
	require.NoError(t, err)
	old, err := legacy.Info(ctx)
	require.NoError(t, err)
	w := &worker.Worker{JetStream: js, Logger: zap.NewNop(), PipelineProgress: server.ApplyPipelineProgressEnvelope,
		PipelineControlResult: server.ApplyPipelineControlResult, DLQPublisher: hnats.NewPublisher(js, hmessaging.DeadLettersStreamName, zap.NewNop())}
	consumer, err := w.BuildRunnerResultsConsumer(ctx, server)
	require.NoError(t, err)
	updated, err := legacy.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, old.Created, updated.Created, "update the existing durable, never reset it")
	require.Contains(t, updated.Config.FilterSubjects, runnerresulthandler.ControlResultSubjects)
	require.Contains(t, updated.Config.FilterSubjects, runnerresulthandler.NativeLogReadySubjects)
	processing, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- consumer.Run(processing) }()
	defer func() {
		stop()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Error("owned normal consumer did not stop")
		}
	}()
	publisher := hnats.NewPublisher(js, hmessaging.RunnerEventsStreamName, zap.NewNop())
	publish := func(event hmessaging.EventEnvelope) []byte {
		raw, err := json.Marshal(event)
		require.NoError(t, err)
		subject, err := hmessaging.RunnerEventSubject(event.OrganizationID, event.RunnerID, event.Type)
		require.NoError(t, err)
		id, err := hmessaging.NativeMessageID(event.OrganizationID, event.RunnerID, "events", event.Type, event.EventID)
		require.NoError(t, err)
		require.NoError(t, publisher.Publish(ctx, hmessaging.Message{ID: id, Subject: subject, Data: raw, CreatedAt: event.CreatedAt}))
		return raw
	}
	result := model.PipelineControlResult{Version: model.PipelineControlResultVersion, ControlID: receipt.ID.String(), RolloutID: dep.Id.String(),
		RequestDigest: receipt.RequestDigest, ExecutionDigest: receipt.ExecutionDigest, Outcome: "applied", Phase: "cancelled", Revision: 2}
	resultJSON, err := json.Marshal(result)
	require.NoError(t, err)
	control := hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, OrganizationID: org, RunnerID: route,
		DeploymentID: dep.Id.String(), Type: model.PipelineControlEventType, EventID: "pipeline-control:" + receipt.ID.String() + ":result",
		CommandID: "pipeline-control:" + receipt.ID.String(), CreatedAt: now, Payload: resultJSON}
	publish(control)
	descriptorJSON, err := json.Marshal(descriptor)
	require.NoError(t, err)
	ready := hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, OrganizationID: org, RunnerID: route,
		DeploymentID: dep.Id.String(), Type: nativelogs.ReadyEventType, EventID: descriptor.EventID(), CreatedAt: now, Payload: descriptorJSON}
	publish(ready)
	require.Eventually(t, func() bool {
		read, err := store.BeginTx(ctx, nil)
		if err != nil {
			return false
		}
		defer func() { _ = read.Rollback() }()
		r, err := model.ReadPipelineControl(ctx, read, org, dep.Id, receipt.ID)
		if err != nil || !bytes.Equal(r.Result, resultJSON) {
			return false
		}
		p, err := model.ReadDeploymentPipeline(ctx, read, dep.Id)
		return err == nil && p != nil && len(p.Progress.LogReferences) == 1 && p.Progress.LogReferences[0] == descriptor
	}, 10*time.Second, 20*time.Millisecond)
	dlq, err := connection.SubscribeSync(hmessaging.DeadLetterSubjectPrefix + ".po.v1.orgs." + org + ".runners.>")
	require.NoError(t, err)
	defer func() { require.NoError(t, dlq.Unsubscribe()) }()
	require.NoError(t, connection.Flush())
	malformed := control
	malformed.EventID, malformed.Payload = "malformed-"+uuid.NewString(), json.RawMessage(`{}`)
	foreign := descriptor
	foreign.Authority.Target.RunnerUUID = uuid.NewString()
	foreign.ObjectKey = foreign.Authority.ObjectKey(foreign.CiphertextSHA256)
	foreignJSON, err := json.Marshal(foreign)
	require.NoError(t, err)
	wrongScope := ready
	wrongScope.RunnerID, err = hmessaging.NativeRunnerToken(foreign.Authority.Target.RunnerUUID)
	require.NoError(t, err)
	wrongScope.Payload, wrongScope.EventID = foreignJSON, foreign.EventID()
	expected := map[string]bool{string(publish(malformed)): true, string(publish(wrongScope)): true}
	for range 2 {
		message, err := dlq.NextMsg(10 * time.Second)
		require.NoError(t, err)
		require.True(t, expected[string(message.Data)], "DLQ must retain the exact malformed or foreign message")
		delete(expected, string(message.Data))
	}
	require.Empty(t, expected)
	require.Equal(t, before, state(), "receipt/reference intake cannot settle infrastructure or change capacity")
}
