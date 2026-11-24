//go:build integration

package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type nativeDispatchCapacity struct {
	t            *testing.T
	mu           sync.Mutex
	registration cp.InternalRunner
	unavailable  bool
	calls        int
}

type nativeDispatchFixture struct {
	noEffectFixture
	dep        *model.DeploymentSummary
	dispatcher *PipelineDispatcher
	capacity   *nativeDispatchCapacity
	broker     nats.JetStreamContext
	subject    string
}

func newNativeDispatchFixture(t *testing.T) *nativeDispatchFixture {
	t.Helper()
	f := &nativeDispatchFixture{noEffectFixture: newNoEffectFixture(t)}
	target := nativeTargetFixture()
	target.RunnerId, target.RunnerUuid = "site", uuid.New()
	backend, digest, err := cp.StateStorageConfigurationDigest(json.RawMessage(`{"type":"kubernetes","namespace":"accepted-state"}`))
	require.NoError(t, err)
	target.StateBackendType, target.StateConfigurationDigest = backend, digest
	f.dep = createNativeDispatchDeployment(t, f.noEffectFixture, target, "env")
	f.capacity = &nativeDispatchCapacity{t: t}
	require.NoError(t, json.Unmarshal([]byte(`{"org_id":"no-effect-org","id":"site","runner_configuration":{"type":"native-agent","max_parallel_jobs":2},"state_storage_configuration":{"type":"s3","bucket":"different-current-state"}}`), &f.capacity.registration))
	f.capacity.registration.Uuid = &target.RunnerUuid
	peer := httptest.NewServer(http.HandlerFunc(f.capacity.serve))
	t.Cleanup(peer.Close)
	f.server.ControlPlaneClient, err = cp.NewClientWithResponses(peer.URL)
	require.NoError(t, err)
	url := os.Getenv("OUTCOME_ACK_TEST_NATS_URL")
	require.NotEmpty(t, url, "an explicit isolated application broker is required")
	connection, err := nats.Connect(url, nats.Timeout(2*time.Second))
	require.NoError(t, err)
	t.Cleanup(connection.Close)
	f.broker, err = connection.JetStream()
	require.NoError(t, err)
	modern, err := jetstream.New(connection)
	require.NoError(t, err)
	_, err = modern.Stream(f.ctx, hmessaging.RunnerCommandsStreamName)
	require.NoError(t, err, "the owned broker must have its normal command stream")
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.dispatcher = &PipelineDispatcher{Server: f.server, Key: key, Publisher: hnats.NewPublisher(modern, hmessaging.RunnerCommandsStreamName, zap.NewNop())}
	route, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	require.NoError(t, err)
	f.subject, err = hmessaging.RunnerCommandSubject(f.dep.OrgId, route)
	require.NoError(t, err)
	return f
}

func (c *nativeDispatchCapacity) update(change func(*nativeDispatchCapacity)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	change(c)
}

func (c *nativeDispatchCapacity) serve(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.unavailable || r.Method != http.MethodGet || r.URL.Path != "/internal/orgs/no-effect-org/runners/site" {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(c.registration); err != nil {
		c.t.Errorf("encode capacity wire response: %v", err)
	}
}

func createNativeDispatchDeployment(t *testing.T, f noEffectFixture, target *cp.NativeExecutionTargetReference, name string) *model.DeploymentSummary {
	t.Helper()
	definition, err := model.ResolvePipeline(nil, model.DeploymentModeDeploy)
	require.NoError(t, err)
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	dep, err := f.server.Database.CreateDeployment(f.ctx, tx, "no-effect-org", "project", name, model.CreateDeploymentParams{
		NativeTarget: target, DeploymentEnvUuid: f.environment, CreatedBy: uuid.New(), Mode: model.DeploymentModeDeploy,
		RunnerId: "site", RunnerLogLevel: "info", Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`),
		Graph: model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), Tofu: model.RawTofu("terraform {}"),
		Pipeline: definition, PipelineBackend: "kubernetes", EncryptedOutputsRecipient: opt.Of("accepted-output-recipient"),
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return dep
}

func (f *nativeDispatchFixture) exec(statement string, args ...any) {
	f.t.Helper()
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(f.ctx, statement, args...)
	require.NoError(f.t, err)
	require.NoError(f.t, tx.Commit())
}

func (f *nativeDispatchFixture) state() string {
	f.t.Helper()
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	defer func() { _ = tx.Rollback() }()
	var value string
	require.NoError(f.t, tx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'pipeline',to_jsonb(p),'budget',to_jsonb(b),'reservation',to_jsonb(r))::text
 FROM deployments d JOIN deployment_pipeline_runs p ON p.deployment_id=d.id JOIN deployment_dispatch_budgets b ON b.deployment_id=d.id
 LEFT JOIN runner_job_reservations r ON r.deployment_id=d.id WHERE d.id=$1`, f.dep.Id).Scan(&value))
	return value
}

func (f *nativeDispatchFixture) last() *nats.RawStreamMsg {
	f.t.Helper()
	message, err := f.broker.GetLastMsg(hmessaging.RunnerCommandsStreamName, f.subject)
	require.NoError(f.t, err)
	return message
}

func TestNativeDispatchPublishesAcceptedAuthorityAndReplaysFrozenBytes(t *testing.T) {
	f := newNativeDispatchFixture(t)
	f.dispatcher.NativeLimits = &model.PipelineExecutionLimits{PlanSeconds: 300, ApplySeconds: 900, TaskSeconds: 450}
	require.Nil(t, f.server.RunnerRollouts, "Native dispatch has no global legacy adapter")
	require.NoError(t, f.dispatcher.Dispatch(f.ctx, f.dep))
	message := f.last()
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(message.Data, &envelope))
	payload, err := model.FrozenPipelinePayload(message.Data, f.dep)
	require.NoError(t, err)
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(payload, &command))
	require.Equal(t, f.dep.NativeTarget, command.NativeTarget)
	require.Equal(t, *f.dispatcher.NativeLimits, *command.ExecutionLimits)
	require.Equal(t, "kubernetes", command.Request.Backend, "current capacity DTO cannot retarget accepted state")
	require.Equal(t, "accepted-output-recipient", command.Request.EncryptedOutputsRecipient)
	require.Empty(t, command.Request.DeploymentToken)
	require.Empty(t, command.Request.MetadataKey)
	id, err := hmessaging.NativeMessageID(f.dep.OrgId, envelope.RunnerID, "commands", envelope.Type, envelope.CommandID)
	require.NoError(t, err)
	require.Equal(t, id, message.Header.Get(nats.MsgIdHdr))
	var signed struct {
		Signature string `json:"signature"`
	}
	require.NoError(t, json.Unmarshal(envelope.Payload, &signed))
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	require.NoError(t, err)
	require.True(t, ed25519.Verify(f.dispatcher.Key.Public().(ed25519.PublicKey), payload, signature))
	before := f.state()
	_, rotated, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.dispatcher.Key, f.dispatcher.ToolsDigest, f.dispatcher.MetadataKey = rotated, "changed-current-tools", "changed-current-metadata"
	f.dispatcher.NativeLimits = &model.PipelineExecutionLimits{ApplySeconds: -1}
	f.capacity.update(func(c *nativeDispatchCapacity) {
		c.unavailable = true
		c.registration.Uuid = nil
	})
	require.NoError(t, f.dispatcher.Dispatch(f.ctx, f.dep), "frozen replay must not fetch current metadata")
	replayed := f.last()
	require.Equal(t, message.Data, replayed.Data)
	require.Equal(t, message.Sequence, replayed.Sequence, "the actual broker recognizes the same message identity")
	require.Equal(t, before, f.state(), "replay cannot refresh time, ownership or dispatch budget")
	f.capacity.update(func(c *nativeDispatchCapacity) { require.Equal(t, 1, c.calls) })
}

func TestNativeDispatchRefusesLostAuthorityWithoutRecreatingOwnership(t *testing.T) {
	for _, trial := range []struct {
		name, statement string
		value           any
	}{
		{"missing ownership", `DELETE FROM runner_job_reservations WHERE deployment_id=$1`, nil},
		{"released ownership", `UPDATE runner_job_reservations SET released_at=now() WHERE deployment_id=$1`, nil},
		{"foreign ownership", `UPDATE runner_job_reservations SET org_id='other-org' WHERE deployment_id=$1`, nil},
		{"stale revision", `UPDATE deployments SET revision=revision+1 WHERE id=$1`, nil},
		{"terminal status", `UPDATE deployments SET status='failed' WHERE id=$1`, nil},
		{"expired budget", `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 days',deadline=now()-interval '1 day' WHERE deployment_id=$1`, nil},
		{"wrong budget revision", `UPDATE deployment_dispatch_budgets SET deployment_revision=deployment_revision+1 WHERE deployment_id=$1`, nil},
		{"completed budget", `UPDATE deployment_dispatch_budgets SET completion_received_at=now() WHERE deployment_id=$1`, nil},
		{"legacy budget", `UPDATE deployment_dispatch_budgets SET legacy=true WHERE deployment_id=$1`, nil},
		{"missing first attempt", `UPDATE deployment_dispatch_budgets SET first_attempt_at=NULL,deadline=NULL WHERE deployment_id=$1`, nil},
		{"malformed command", `UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, []byte(`{}`)},
		{"changed backend", `UPDATE deployment_pipeline_runs SET backend='s3' WHERE deployment_id=$1`, nil},
		{"changed retained target", `UPDATE deployments SET native_target=jsonb_set(native_target,'{runner_uuid}',to_jsonb($2::text)) WHERE id=$1`, uuid.NewString()},
	} {
		t.Run(trial.name, func(t *testing.T) {
			f := newNativeDispatchFixture(t)
			require.NoError(t, f.dispatcher.Dispatch(f.ctx, f.dep))
			published := f.last()
			args := []any{f.dep.Id}
			if trial.value != nil {
				args = append(args, trial.value)
			}
			f.exec(trial.statement, args...)
			before := f.state()
			require.Error(t, f.dispatcher.Dispatch(f.ctx, f.dep))
			require.Equal(t, before, f.state(), "refusal cannot rewrite or recreate authority")
			require.Equal(t, published.Sequence, f.last().Sequence, "refusal cannot publish")
			f.capacity.update(func(c *nativeDispatchCapacity) {
				require.Equal(t, 1, c.calls, "frozen refusal precedes current registration lookup")
			})
		})
	}
}

func TestNativeDispatchNewReservationRequiresExactRegistrationAndPositiveCapacity(t *testing.T) {
	for _, trial := range []string{"zero capacity", "missing capacity", "new UUID", "missing UUID", "foreign org", "foreign Runner"} {
		t.Run(trial, func(t *testing.T) {
			f := newNativeDispatchFixture(t)
			f.capacity.update(func(c *nativeDispatchCapacity) {
				switch trial {
				case "zero capacity":
					require.NoError(t, json.Unmarshal([]byte(`{"type":"native-agent","max_parallel_jobs":0}`), &c.registration.RunnerConfiguration))
				case "missing capacity":
					require.NoError(t, json.Unmarshal([]byte(`{"type":"native-agent"}`), &c.registration.RunnerConfiguration))
				case "new UUID":
					id := uuid.New()
					c.registration.Uuid = &id
				case "missing UUID":
					c.registration.Uuid = nil
				case "foreign org":
					c.registration.OrgId = "another-org"
				case "foreign Runner":
					c.registration.Id = "another-site"
				}
			})
			before := f.state()
			require.Error(t, f.dispatcher.Dispatch(f.ctx, f.dep))
			require.Equal(t, before, f.state())
			_, err := f.broker.GetLastMsg(hmessaging.RunnerCommandsStreamName, f.subject)
			require.ErrorIs(t, err, nats.ErrMsgNotFound)
		})
	}
}

func TestNativeDispatchReplicasRaceForOneRemainingSlot(t *testing.T) {
	f := newNativeDispatchFixture(t)
	f.capacity.update(func(c *nativeDispatchCapacity) {
		require.NoError(t, json.Unmarshal([]byte(`{"type":"native-agent","max_parallel_jobs":1}`), &c.registration.RunnerConfiguration))
	})
	other := f.noEffectFixture
	other.environment = uuid.New()
	second := createNativeDispatchDeployment(t, other, f.dep.NativeTarget, "another-env")
	results := make(chan error, 2)
	var group sync.WaitGroup
	for _, dep := range []*model.DeploymentSummary{f.dep, second} {
		group.Go(func() { results <- f.dispatcher.Dispatch(f.ctx, dep) })
	}
	group.Wait()
	close(results)
	passed, full := 0, 0
	for err := range results {
		if err == nil {
			passed++
		} else if errors.Is(err, model.ErrRunnerCapacityFull) {
			full++
		} else {
			t.Fatalf("unexpected race result: %v", err)
		}
	}
	require.Equal(t, 1, passed)
	require.Equal(t, 1, full)
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var reservations, commands int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_job_reservations WHERE org_id=$1 AND runner_id=$2 AND released_at IS NULL`, f.dep.OrgId, f.dep.RunnerId).Scan(&reservations))
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_pipeline_runs WHERE command IS NOT NULL`).Scan(&commands))
	require.Equal(t, 1, reservations)
	require.Equal(t, 1, commands)
}
