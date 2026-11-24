//go:build integration

package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newNativeObservationDispatchFixture(t *testing.T) *nativeDispatchFixture {
	t.Helper()
	f := &nativeDispatchFixture{noEffectFixture: newNoEffectFixture(t)}
	target := nativeTargetFixture()
	target.RunnerId, target.RunnerUuid = "site", uuid.New()
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	f.dep, err = f.server.Database.CreateDeployment(f.ctx, tx, "no-effect-org", "project", "env", model.CreateDeploymentParams{
		NativeTarget: target, DeploymentEnvUuid: f.environment, CreatedBy: uuid.New(), Mode: model.DeploymentModeObservation,
		RunnerId: "site", RunnerLogLevel: "info", Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`),
		Graph: model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), Tofu: model.RawTofu("terraform {}"),
	})
	require.NoError(t, err)
	require.NoError(t, model.ReserveRunnerJob(f.ctx, tx, f.dep, 2))
	require.NoError(t, tx.Commit())
	// This is an accepted-state fixture, not a bypass of the public admission
	// guard. The observer has its admission reservation and no pipeline row.
	f.capacity = &nativeDispatchCapacity{t: t, unavailable: true}
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
	require.NoError(t, err)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.dispatcher = &PipelineDispatcher{Server: f.server, Key: key, Publisher: hnats.NewPublisher(modern, hmessaging.RunnerCommandsStreamName, zap.NewNop())}
	route, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	require.NoError(t, err)
	f.subject, err = hmessaging.RunnerCommandSubject(f.dep.OrgId, route)
	require.NoError(t, err)
	return f
}

func nativeObservationDispatchState(t *testing.T, f *nativeDispatchFixture) string {
	t.Helper()
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var value string
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT jsonb_build_object('deployment',to_jsonb(d),'budget',to_jsonb(b),'reservation',to_jsonb(r),
 'pipeline_rows',(SELECT count(*) FROM deployment_pipeline_runs WHERE deployment_id=d.id))::text
 FROM deployments d JOIN deployment_dispatch_budgets b ON b.deployment_id=d.id
 LEFT JOIN runner_job_reservations r ON r.deployment_id=d.id WHERE d.id=$1`, f.dep.Id).Scan(&value))
	return value
}

func TestNativeObservationDispatchUsesCapturedAuthorityAndFrozenReplay(t *testing.T) {
	f := newNativeObservationDispatchFixture(t)
	require.False(t, f.dep.HasNativeObservation, "first dispatch has no frozen-command marker")
	require.False(t, f.dep.HasPipeline)
	require.Nil(t, f.server.RunnerRollouts)
	require.NoError(t, f.dispatcher.DispatchObservation(f.ctx, f.dep))
	message := f.last()
	require.NoError(t, validateFrozenObservationCommand(message.Data, f.dep))
	var envelope hmessaging.CommandEnvelope
	require.NoError(t, json.Unmarshal(message.Data, &envelope))
	payload, err := model.FrozenPipelinePayload(message.Data, f.dep)
	require.NoError(t, err)
	var command pipelineCommand
	require.NoError(t, json.Unmarshal(payload, &command))
	require.Equal(t, f.dep.NativeTarget, command.NativeTarget)
	require.Equal(t, string(f.dep.NativeTarget.StateBackendType), command.Request.Backend)
	require.Empty(t, command.Request.DeploymentToken)
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
	// Preparation age applies only before the first frozen command. Its signed
	// lifetime, not a fresh preparation window, controls subsequent dispatch.
	f.exec(`UPDATE deployments SET created_at=now()-interval '2 hours' WHERE id=$1`, f.dep.Id)
	before := nativeObservationDispatchState(t, f)
	_, rotated, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	f.dispatcher.Key, f.dispatcher.MetadataKey, f.dispatcher.ToolsDigest = rotated, "changed", "changed"
	require.NoError(t, f.dispatcher.DispatchObservation(f.ctx, f.dep))
	require.Equal(t, message.Data, f.last().Data)
	require.Equal(t, message.Sequence, f.last().Sequence)
	require.Equal(t, before, nativeObservationDispatchState(t, f))
	f.capacity.update(func(c *nativeDispatchCapacity) {
		require.Zero(t, c.calls, "neither first dispatch nor replay can consult current registration")
	})
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var reservations, pipelines int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_job_reservations WHERE deployment_id=$1 AND released_at IS NULL`, f.dep.Id).Scan(&reservations))
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_pipeline_runs WHERE deployment_id=$1`, f.dep.Id).Scan(&pipelines))
	require.Equal(t, 1, reservations)
	require.Zero(t, pipelines)
}

func TestNativeObservationDispatchRefusesChangedAuthorityWithoutRewriting(t *testing.T) {
	for _, trial := range []struct {
		name, statement string
		value           any
	}{
		{"missing ownership", `DELETE FROM runner_job_reservations WHERE deployment_id=$1`, nil},
		{"released ownership", `UPDATE runner_job_reservations SET released_at=now() WHERE deployment_id=$1`, nil},
		{"foreign ownership", `UPDATE runner_job_reservations SET org_id='foreign' WHERE deployment_id=$1`, nil},
		{"revision", `UPDATE deployments SET revision=revision+1 WHERE id=$1`, nil},
		{"status", `UPDATE deployments SET status='failed' WHERE id=$1`, nil},
		{"target", `UPDATE deployments SET native_target=jsonb_set(native_target,'{runner_uuid}',to_jsonb($2::text)) WHERE id=$1`, uuid.NewString()},
		{"budget revision", `UPDATE deployment_dispatch_budgets SET deployment_revision=deployment_revision+1 WHERE deployment_id=$1`, nil},
		{"budget expiry", `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 days',deadline=now()-interval '1 day' WHERE deployment_id=$1`, nil},
		{"completion", `UPDATE deployment_dispatch_budgets SET completion_received_at=now() WHERE deployment_id=$1`, nil},
		{"legacy budget", `UPDATE deployment_dispatch_budgets SET legacy=true WHERE deployment_id=$1`, nil},
		{"lost budget", `UPDATE deployment_dispatch_budgets SET first_attempt_at=NULL,deadline=NULL WHERE deployment_id=$1`, nil},
		{"malformed command", `UPDATE deployment_dispatch_budgets SET observation_command=$2 WHERE deployment_id=$1`, []byte(`{}`)},
	} {
		t.Run(trial.name, func(t *testing.T) {
			f := newNativeObservationDispatchFixture(t)
			require.NoError(t, f.dispatcher.DispatchObservation(f.ctx, f.dep))
			published := f.last()
			args := []any{f.dep.Id}
			if trial.value != nil {
				args = append(args, trial.value)
			}
			f.exec(trial.statement, args...)
			before := nativeObservationDispatchState(t, f)
			require.Error(t, f.dispatcher.DispatchObservation(f.ctx, f.dep))
			require.Equal(t, before, nativeObservationDispatchState(t, f))
			require.Equal(t, published.Sequence, f.last().Sequence)
			f.capacity.update(func(c *nativeDispatchCapacity) { require.Zero(t, c.calls) })
		})
	}
}

func TestNativeObservationFirstDispatchRequiresAdmissionOwnershipAndPreparationWindow(t *testing.T) {
	for _, statement := range []string{
		`DELETE FROM runner_job_reservations WHERE deployment_id=$1`,
		`UPDATE runner_job_reservations SET released_at=now() WHERE deployment_id=$1`,
		`UPDATE deployments SET created_at=now()-interval '2 hours' WHERE id=$1`,
	} {
		f := newNativeObservationDispatchFixture(t)
		f.exec(statement, f.dep.Id)
		before := nativeObservationDispatchState(t, f)
		require.Error(t, f.dispatcher.DispatchObservation(f.ctx, f.dep))
		require.Equal(t, before, nativeObservationDispatchState(t, f))
		_, err := f.broker.GetLastMsg(hmessaging.RunnerCommandsStreamName, f.subject)
		require.ErrorIs(t, err, nats.ErrMsgNotFound)
		f.capacity.update(func(c *nativeDispatchCapacity) { require.Zero(t, c.calls) })
	}
}

func TestNativeObservationFinalLockedRevisionPrecedesFreeze(t *testing.T) {
	f := newNativeObservationDispatchFixture(t)
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var holder int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT pg_backend_pid() FROM deployments WHERE id=$1 FOR UPDATE`, f.dep.Id).Scan(&holder))
	done := make(chan error, 1)
	go func() { done <- f.dispatcher.DispatchObservation(f.ctx, f.dep) }()
	require.Eventually(t, func() bool {
		probe, err := f.server.Database.BeginTx(f.ctx, nil)
		if err != nil {
			return false
		}
		defer func() { _ = probe.Rollback() }()
		var blocked bool
		err = probe.QueryRowContext(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, holder).Scan(&blocked)
		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET revision=revision+1 WHERE id=$1`, f.dep.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.Error(t, <-done)
	_, err = f.broker.GetLastMsg(hmessaging.RunnerCommandsStreamName, f.subject)
	require.ErrorIs(t, err, nats.ErrMsgNotFound)
	f.capacity.update(func(c *nativeDispatchCapacity) { require.Zero(t, c.calls) })
}

func TestNativeObservationRetainedCustomerStepCannotBePublished(t *testing.T) {
	f := newNativeObservationDispatchFixture(t)
	require.NoError(t, f.dispatcher.DispatchObservation(f.ctx, f.dep))
	published := f.last()
	changed := resignObservationFixture(t, published.Data, f.dispatcher.Key, func(c *pipelineCommand) {
		c.Steps = []pipelineCommandStep{{ID: "customer-task", ToolID: "local.exec"}}
	})
	f.exec(`UPDATE deployment_dispatch_budgets SET observation_command=$2 WHERE deployment_id=$1`, f.dep.Id, changed)
	before := nativeObservationDispatchState(t, f)
	require.Error(t, f.dispatcher.DispatchObservation(f.ctx, f.dep))
	require.Equal(t, before, nativeObservationDispatchState(t, f))
	require.Equal(t, published.Sequence, f.last().Sequence)
}
