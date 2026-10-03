//go:build integration

package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

type lateResultFixture struct {
	ctx           context.Context
	store         model.Databaser
	server        *Server
	base, forward *model.DeploymentSummary
	params        model.CreateDeploymentParams
	body          DeploymentResultsUpdateBody
	envelope      hmessaging.EventEnvelope
	project       uuid.UUID
	identity      age.Identity
}

func newLateResultFixture(t *testing.T) lateResultFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	store := externalDeploymentTestDatabase(t, ctx, connection)
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	var ciphertext bytes.Buffer
	writer, err := age.Encrypt(&ciphertext, identity.Recipient())
	require.NoError(t, err)
	_, err = writer.Write([]byte(`{"private":"protected output"}`))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	outputs := base64.StdEncoding.EncodeToString(ciphertext.Bytes())
	params := model.CreateDeploymentParams{CreatedBy: uuid.New(), DeploymentEnvUuid: uuid.New(), Mode: model.DeploymentModeDeploy, RunnerId: "runner", RunnerLogLevel: "info",
		Manifest: model.EncodedDeploymentManifest(`{"workloads":{}}`), Graph: model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`), Tofu: model.RawTofu("terraform {}"), EncryptedOutputsRecipient: opt.Of(identity.Recipient().String())}
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	base, err := store.CreateDeployment(ctx, tx, "late-org", "p", "e", params)
	require.NoError(t, err)
	_, err = store.UpdateDeploymentStatusAndOutputs(ctx, tx, base.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded})
	require.NoError(t, err)
	forward, err := store.CreateDeployment(ctx, tx, "late-org", "p", "e", params)
	require.NoError(t, err)
	operation, project := uuid.New(), uuid.New()
	_, err = model.StoreExternalOperationMaterial(ctx, tx, model.ExternalOperationMaterial{OrgID: "late-org", PluginID: "dev.example.rollouts", OperationRef: operation, Recipient: identity.Recipient().String(), EncryptedIdentity: "vault:v1:wrapped"})
	require.NoError(t, err)
	_, err = model.CreateExternalOperationDeploymentReceipt(ctx, tx, model.ExternalOperationDeploymentReceipt{
		ExternalOperationDeploymentKey: model.ExternalOperationDeploymentKey{OrgID: "late-org", PluginID: "dev.example.rollouts", RequestID: "original-dispatch"},
		OperationRef:                   operation.String(),
		Fence:                          3,
		AssertionAction:                ExternalDeploymentAction,
		ServiceAction:                  ExternalDeploymentAction,
		RequestBodySHA256:              strings.Repeat("a", 64),
		ActorID:                        params.CreatedBy.String(),
		ProjectID:                      "p",
		EnvID:                          "e",
		ProjectUUID:                    opt.Of(project),
		EnvironmentUUID:                opt.Of(params.DeploymentEnvUuid),
		BaseDeploymentID:               opt.Of(base.Id),
		ForwardDeploymentID:            opt.Of(forward.Id),
		DeploymentID:                   forward.Id,
	})
	require.NoError(t, err)
	require.NoError(
		t,
		model.CreateExternalOperationRollbackAnchor(
			ctx,
			tx,
			model.ExternalOperationRollbackAnchor{
				OrgID:                    "late-org",
				PluginID:                 "dev.example.rollouts",
				OperationRef:             operation.String(),
				ProjectID:                "p",
				EnvID:                    "e",
				ProjectUUID:              project,
				EnvironmentUUID:          params.DeploymentEnvUuid,
				FirstForwardDeploymentID: forward.Id,
				RollbackBaseDeploymentID: base.Id,
			},
		),
	)
	require.NoError(t, tx.Commit())
	_, err = model.BeginDeploymentDispatch(ctx, store, forward)
	require.NoError(t, err)
	body := DeploymentResultsUpdateBody{Status: Success, Outputs: &outputs}
	payload, err := json.Marshal(body)
	require.NoError(t, err)
	envelope := hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, EventID: forward.Id.String() + ":deployment-result", OrganizationID: "late-org", RunnerID: "runner", DeploymentID: forward.Id.String(), Type: "deployment-result", CreatedAt: time.Now().UTC(), Payload: payload}
	return lateResultFixture{
		ctx:      ctx,
		store:    store,
		server:   &Server{Database: store, Logger: zap.NewNop(), EventPublisher: &hmessaging.RecordingPublisher{}, RunnerTokenSalt: "test-salt"},
		base:     base,
		forward:  forward,
		params:   params,
		body:     body,
		envelope: envelope,
		project:  project,
		identity: identity,
	}
}

func (f lateResultFixture) fail(t *testing.T) *model.DeploymentSummary {
	t.Helper()
	failed, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, f.forward.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed, StatusMessage: "watchdog timeout", ExpectedRevision: opt.Of(int64(f.forward.Revision))})
	require.NoError(t, err)
	require.NoError(t, f.store.CreateDeploymentHistoryRecord(f.ctx, nil, failed))
	return failed
}

func TestGenuineLateResultRetainsFailureAndEnablesExactOwnedRollback(t *testing.T) {
	f := newLateResultFixture(t)
	failed := f.fail(t)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	unknown, err := model.DeploymentOutcomeUnreconciled(f.ctx, tx, f.forward.Id)
	require.NoError(t, err)
	require.True(t, unknown)
	require.NoError(t, tx.Rollback())
	adapter := ExternalDeploymentHTTPAdapter{Admission: ExternalDeploymentAdmissionService{Database: f.store}}
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	receipt, err := model.ReadExternalOperationDeploymentReceiptByDeployment(f.ctx, tx, "late-org", "dev.example.rollouts", f.forward.Id)
	require.NoError(t, err)
	_, err = adapter.projectExternalDeploymentStatus(f.ctx, tx, *failed, *receipt, f.project, f.params.DeploymentEnvUuid)
	require.ErrorIs(t, err, errDeploymentOutcomeUnreconciled)
	require.NoError(t, tx.Rollback())
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.envelope, f.body))
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.envelope, f.body), "exact duplicate is no effect")
	got, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, "late-org", f.forward.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, failed.Status, got.Status)
	require.Equal(t, failed.CompletedAt, got.CompletedAt)
	require.Equal(t, failed.Revision, got.Revision)
	require.Equal(t, failed.StatusMessage, got.StatusMessage)
	require.Equal(t, f.params.Manifest, manifest)
	require.Equal(t, f.params.Graph, graph)
	require.Equal(t, f.params.Tofu, tofu)
	encoded, err := f.store.GetDeploymentEncryptedOutputs(f.ctx, nil, "late-org", f.forward.Id)
	require.NoError(t, err)
	require.Equal(t, *f.body.Outputs, encoded)
	plaintext, err := decryptExternalDeploymentPayload(strings.NewReader(encoded), f.identity, maxExternalMaterialBytes)
	require.NoError(t, err)
	require.JSONEq(t, `{"private":"protected output"}`, string(plaintext))
	_, err = f.store.GetDeploymentEncryptedOutputs(f.ctx, nil, "foreign-org", f.forward.Id)
	require.Error(t, err)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	var histories, results, inbox int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_history WHERE id=$1`, f.forward.Id).Scan(&histories))
	require.Equal(t, 1, histories)
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_late_runner_results WHERE deployment_id=$1`, f.forward.Id).Scan(&results))
	require.Equal(t, 1, results)
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_event_inbox WHERE deployment_id=$1`, f.forward.Id).Scan(&inbox))
	require.Zero(t, inbox, "late result is not falsely recorded as an applied terminal update")
	late, err := lateSuccessfulOutputs(f.ctx, tx, *got)
	require.NoError(t, err)
	require.True(t, late)
	unknown, err = model.DeploymentOutcomeUnreconciled(f.ctx, tx, f.forward.Id)
	require.NoError(t, err)
	require.False(t, unknown)
	projected, err := adapter.projectExternalDeploymentStatus(f.ctx, tx, *got, *receipt, f.project, f.params.DeploymentEnvUuid)
	require.NoError(t, err)
	require.Equal(t, "failed", projected.Status)
	recovery := f.params
	recovery.Mode = model.DeploymentModeRollback
	recovery.RollbackToId = opt.Of(f.base.Id)
	recovery.ExpectedCurrentDeploymentID = opt.Of(f.forward.Id)
	rollback, err := f.store.CreateDeployment(f.ctx, tx, "late-org", "p", "e", recovery)
	require.NoError(t, err)
	require.Equal(t, f.base.Id, rollback.RollbackToId.Must())
	require.NoError(t, tx.Commit())
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.envelope, f.body), "exact retained duplicate remains safe after recovery changed the head")
	conflicting := f.envelope
	changed := f.body
	changed.Status = Failure
	conflicting.Payload, err = json.Marshal(changed)
	require.NoError(t, err)
	require.ErrorContains(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, conflicting, changed), "immutable")
}

func TestResultRacingWatchdogRetainsEvidenceWithoutRewritingFailure(t *testing.T) {
	f := newLateResultFixture(t)
	blocker, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = blocker.Rollback() }()
	_, _, _, _, err = f.store.GetDeployment(f.ctx, blocker, "late-org", f.forward.Id, model.GetModeForUpdate)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.envelope, f.body) }()
	require.Eventually(t, func() bool {
		var waiting bool
		if _, err := blocker.ExecContext(f.ctx, `SELECT pg_stat_clear_snapshot()`); err != nil {
			return false
		}
		err := blocker.QueryRowContext(f.ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%FROM deployments d INNER JOIN deployment_environments%' AND query LIKE '%FOR UPDATE%')`).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond, "normal result must have peeked executing then waited behind the real watchdog row lock")
	failed, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, blocker, f.forward.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed, StatusMessage: "watchdog", ExpectedRevision: opt.Of(int64(1))})
	require.NoError(t, err)
	require.NoError(t, f.store.CreateDeploymentHistoryRecord(f.ctx, blocker, failed))
	require.NoError(t, blocker.Commit())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-f.ctx.Done():
		t.Fatal("result did not finish after watchdog committed")
	}
	got, _, _, _, err := f.store.GetDeployment(f.ctx, nil, "late-org", f.forward.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentStatusFailed, got.Status)
	require.Equal(t, failed.CompletedAt, got.CompletedAt)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var retained int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_late_runner_results WHERE deployment_id=$1`, f.forward.Id).Scan(&retained))
	require.Equal(t, 1, retained)
}

func TestLateCiphertextFollowsExistingExactMaterialRetention(t *testing.T) {
	f := newLateResultFixture(t)
	f.fail(t)
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.envelope, f.body))
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET completed_at=now()-interval '10 minutes' WHERE id=$1`, f.forward.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = f.store.DeleteDeploymentOutputsByCompletionDate(f.ctx, nil, time.Now().Add(-5*time.Minute))
	require.NoError(t, err)
	encoded, err := f.store.GetDeploymentEncryptedOutputs(f.ctx, nil, "late-org", f.forward.Id)
	require.NoError(t, err)
	require.Equal(t, *f.body.Outputs, encoded)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE external_operation_material SET recipient='foreign' WHERE org_id='late-org'`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	_, err = f.store.DeleteDeploymentOutputsByCompletionDate(f.ctx, nil, time.Now().Add(-5*time.Minute))
	require.NoError(t, err)
	encoded, err = f.store.GetDeploymentEncryptedOutputs(f.ctx, nil, "late-org", f.forward.Id)
	require.NoError(t, err)
	require.Empty(t, encoded)
	tx, err = f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var evidence int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_late_runner_results WHERE deployment_id=$1 AND encrypted_outputs IS NULL AND payload_sha256<>''`, f.forward.Id).Scan(&evidence))
	require.Equal(t, 1, evidence, "neutral stop/digest evidence survives ciphertext cleanup")
}

func TestLateCompletionSerializesCapturedBaseAdmission(t *testing.T) {
	f := newLateResultFixture(t)
	f.fail(t)
	start := make(chan struct{})
	lateResult := make(chan error, 1)
	admission := make(chan error, 1)
	go func() {
		<-start
		lateResult <- f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.envelope, f.body)
	}()
	go func() {
		<-start
		tx, err := f.store.BeginTx(f.ctx, nil)
		if err != nil {
			admission <- err
			return
		}
		defer func() { _ = tx.Rollback() }()
		recovery := f.params
		recovery.Mode = model.DeploymentModeRollback
		recovery.RollbackToId = opt.Of(f.base.Id)
		recovery.ExpectedCurrentDeploymentID = opt.Of(f.forward.Id)
		_, err = f.store.CreateDeployment(f.ctx, tx, "late-org", "p", "e", recovery)
		if err == nil {
			err = tx.Commit()
		}
		admission <- err
	}()
	close(start)
	require.NoError(t, <-lateResult)
	admissionErr := <-admission
	if admissionErr != nil {
		_, conflict := model.IsErrConflict(admissionErr)
		require.True(t, conflict, admissionErr.Error())
	}
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var results, rollbacks int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_late_runner_results WHERE deployment_id=$1`, f.forward.Id).Scan(&results))
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployments WHERE mode='rollback'`).Scan(&rollbacks))
	require.Equal(t, 1, results)
	if admissionErr == nil {
		require.Equal(t, 1, rollbacks)
	} else {
		require.Zero(t, rollbacks, "unreconciled admission cannot race past the stop proof")
	}
	failed, _, _, _, err := f.store.GetDeployment(f.ctx, tx, "late-org", f.forward.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentStatusFailed, failed.Status)
}

func TestLateResultRejectsForeignBindingsAndErrorOnlyStop(t *testing.T) {
	for _, name := range []string{"runner", "organization", "event", "command", "time", "missing receipt", "receipt environment", "anchor base", "incomplete base", "foreign lease", "foreign head", "plaintext outputs"} {
		t.Run(name, func(t *testing.T) {
			f := newLateResultFixture(t)
			f.fail(t)
			envelope := f.envelope
			tx, err := f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			switch name {
			case "runner":
				envelope.RunnerID = "foreign"
			case "organization":
				envelope.OrganizationID = "foreign"
			case "event":
				envelope.EventID = "foreign"
			case "command":
				envelope.CommandID = "create-job:" + f.forward.Id.String() + ":7"
			case "time":
				envelope.CreatedAt = f.forward.CreatedAt.Add(-time.Hour)
			case "missing receipt":
				_, err = tx.ExecContext(f.ctx, `DELETE FROM external_operation_deployments WHERE deployment_id=$1`, f.forward.Id)
			case "receipt environment":
				_, err = tx.ExecContext(f.ctx, `UPDATE external_operation_deployments SET environment_uuid=$2 WHERE deployment_id=$1`, f.forward.Id, uuid.New())
			case "anchor base":
				_, err = tx.ExecContext(f.ctx, `UPDATE external_operation_rollback_anchors SET rollback_base_deployment_id=$1`, uuid.New())
			case "incomplete base":
				_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET graph='null'::bytea WHERE id=$1`, f.base.Id)
			case "foreign lease":
				_, err = tx.ExecContext(f.ctx, `INSERT INTO environment_deployment_leases(environment_id,org_id,project_id,env_id,holder_id,purpose,deployment_id,expires_at) VALUES($1,'late-org','p','e',$2,'deployment',$2,now()+interval '1 hour')`, f.params.DeploymentEnvUuid, uuid.New())
			case "foreign head":
				_, err = tx.ExecContext(f.ctx, `UPDATE deployment_environments SET last_state_deployment_id=$2 WHERE id=$1`, f.params.DeploymentEnvUuid, f.base.Id)
			}
			if name == "plaintext outputs" {
				plain := base64.StdEncoding.EncodeToString([]byte("secret plaintext"))
				f.body.Outputs = &plain
				envelope.Payload, err = json.Marshal(f.body)
			}
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			require.Error(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, envelope, f.body))
			tx, err = f.store.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			var count int
			require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM deployment_late_runner_results`).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, tx.Rollback())
		})
	}
}

func TestForceFailIsNotCompletionButAuthenticatedHTTPResultIs(t *testing.T) {
	t.Run("force fail", func(t *testing.T) {
		f := newLateResultFixture(t)
		ctx := context.WithValue(f.ctx, hecho.ContextKeyUserID, userid.InternalSystemUuid.String())
		_, err := f.server.InternalForceFailDeployment(ctx, InternalForceFailDeploymentRequestObject{OrgId: "late-org", DeploymentId: f.forward.Id})
		require.NoError(t, err)
		tx, err := f.store.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		unknown, err := model.DeploymentOutcomeUnreconciled(f.ctx, tx, f.forward.Id)
		require.NoError(t, err)
		require.True(t, unknown)
	})
	t.Run("authenticated HTTP", func(t *testing.T) {
		f := newLateResultFixture(t)
		body := DeploymentResultsUpdateBody{Status: Failure}
		_, err := f.server.UpdateDeploymentResults(f.ctx, UpdateDeploymentResultsRequestObject{OrgId: "late-org", DeploymentId: f.forward.Id, Body: &body, Params: UpdateDeploymentResultsParams{XDeploymentToken: util.GenerateHashedRunnerToken("test-salt", "late-org", f.forward.Id.String())}})
		require.NoError(t, err)
		tx, err := f.store.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		unknown, err := model.DeploymentOutcomeUnreconciled(f.ctx, tx, f.forward.Id)
		require.NoError(t, err)
		require.False(t, unknown)
		recovery := f.params
		recovery.Mode = model.DeploymentModeRollback
		recovery.RollbackToId = opt.Of(f.base.Id)
		recovery.ExpectedCurrentDeploymentID = opt.Of(f.forward.Id)
		_, err = f.store.CreateDeployment(f.ctx, tx, "late-org", "p", "e", recovery)
		require.NoError(t, err)
	})
}
