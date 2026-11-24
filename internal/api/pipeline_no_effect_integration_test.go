//go:build integration

package api

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPipelineNoEffectKeepsEffectiveBaseAndCollapsesFailedChains(t *testing.T) {
	f := newNoEffectFixture(t)
	base := f.create(false)
	_, err := f.server.Database.UpdateDeploymentStatusAndOutputs(f.ctx, nil, base.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	for range 2 {
		failed, receipt := f.pipeline()
		body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
		event := f.event(failed, body)
		require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, event, body))
		require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, event, body), "same receipt is idempotent")
		f.requireEffective(base.Id)
		saved, _, _, _, err := f.server.Database.GetDeployment(f.ctx, nil, failed.OrgId, failed.Id, model.GetModeDefault)
		require.NoError(t, err)
		require.Equal(t, model.DeploymentStatusFailed, saved.Status, "failure history is preserved")
		tx, err := f.server.Database.BeginTx(f.ctx, nil)
		require.NoError(t, err)
		var captured, operational uuid.UUID
		require.NoError(
			t,
			tx.QueryRowContext(f.ctx, `SELECT p.effect_base_deployment_id,e.last_state_deployment_id FROM deployment_pipeline_runs p JOIN deployments d ON d.id=p.deployment_id JOIN deployment_environments e ON e.id=d.de_id WHERE p.deployment_id=$1`, failed.Id).Scan(&captured, &operational),
		)
		require.Equal(t, base.Id, captured, "predecessor is already resolved, never another no-effect failure")
		require.Equal(t, failed.Id, operational, "operational identity is retained")
		require.NoError(t, tx.Rollback())
		progress := model.PipelineProgress{ID: failed.Id.String(), EnvironmentID: f.environment.String(), Revision: 99, Phase: "completed", InfrastructureApplied: true}
		raw, err := json.Marshal(map[string]any{"progress": progress})
		require.NoError(t, err)
		require.ErrorIs(t, f.server.ApplyPipelineProgressEnvelope(f.ctx, hmessaging.EventEnvelope{OrganizationID: failed.OrgId, RunnerID: failed.RunnerId, DeploymentID: failed.Id.String(), Payload: raw}), ErrRunnerEventTargetInvalid)
		f.requireEffective(base.Id)
	}
	ready, err := f.server.Database.GetEnvironmentDeploymentReadiness(f.ctx, nil, "no-effect-org", []model.EnvironmentRef{{ProjectID: "project", EnvID: "env"}})
	require.NoError(t, err)
	require.Equal(t, base.Id, ready[0].LatestDeploymentID.Must())
	now := time.Now().UTC()
	observed, _, err := model.TryStartObservation(
		f.ctx,
		f.server.Database,
		"no-effect-org",
		"dev.example.drift",
		uuid.New(),
		observation.ObservationRequest{GrantId: uuid.New(), GrantRevision: 1, ProjectId: "project", EnvId: "env", ProjectUuid: uuid.New(), EnvironmentUuid: f.environment, RequestId: uuid.New(), Occurrence: now, AdmissionDeadline: now.Add(time.Minute)},
		"site",
		2,
		true,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, observed.Disposition)
	require.Equal(t, base.Id, *observed.BaselineId, "normal observation admission uses the unchanged actual base")
}

func TestPipelineNoEffectReceiptCanEnrichPriorCompletionWithoutResettingNewerHead(t *testing.T) {
	f := newNoEffectFixture(t)
	failed, receipt := f.pipeline()
	original := DeploymentResultsUpdateBody{Status: Failure}
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, original), original))
	f.requireEffective(failed.Id, "an unqualified failure never implies no effect")
	next := f.create(false)
	_, err := f.server.Database.UpdateDeploymentStatusAndOutputs(f.ctx, nil, next.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	qualified := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, qualified), qualified))
	f.requireEffective(next.Id)
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	var count int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM pipeline_no_effect_receipts WHERE deployment_id=$1`, failed.Id).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, tx.Rollback())
}

func TestPipelineNoEffectInitialEmptyBaseAndLateProofNeverResetNewerHead(t *testing.T) {
	f := newNoEffectFixture(t)
	failed, receipt := f.pipeline()
	body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, body), body))
	f.requireEffective(uuid.Nil)
	coverage, err := scanProviderUsage(f.ctx, f.server.Database, failed.OrgId, []uuid.UUID{f.environment})
	require.NoError(t, err)
	require.True(t, coverage.Complete)
	require.Equal(t, []uuid.UUID{f.environment}, coverage.EnvironmentUuids)
	require.Empty(t, coverage.Providers)
	next := f.create(false)
	_, err = f.server.Database.UpdateDeploymentStatusAndOutputs(f.ctx, nil, next.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	event := f.event(failed, body)
	event.EventID += ":late-copy"
	_, err = f.server.commonUpdateDeploymentResults(
		f.ctx,
		zap.NewNop(),
		UpdateDeploymentResultsRequestObject{OrgId: failed.OrgId, DeploymentId: failed.Id, Body: &body},
		model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed},
		&RunnerEventMetadata{EventID: event.EventID, RunnerID: failed.RunnerId, Type: "deployment-result"},
		true,
	)
	require.Error(t, err, "a different terminal event cannot rewrite failure history")
	f.requireEffective(next.Id)
}

func TestPipelineNoEffectSerializesReplacementAdmission(t *testing.T) {
	f := newNoEffectFixture(t)
	failed, receipt := f.pipeline()
	body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
	event := f.event(failed, body)
	var completion, admission error
	var next *model.DeploymentSummary
	start := make(chan struct{})
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		<-start
		completion = f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, event, body)
	}()
	go func() {
		defer group.Done()
		<-start
		tx, err := f.server.Database.BeginTx(f.ctx, nil)
		if err != nil {
			admission = err
			return
		}
		defer func() { _ = tx.Rollback() }()
		next, admission = f.server.Database.CreateDeployment(
			f.ctx,
			tx,
			"no-effect-org",
			"project",
			"env",
			model.CreateDeploymentParams{
				DeploymentEnvUuid: f.environment,
				Mode:              model.DeploymentModeDeploy,
				CreatedBy:         uuid.New(),
				RunnerId:          "site",
				RunnerLogLevel:    "info",
				Manifest:          model.EncodedDeploymentManifest(`{}`),
				Graph:             model.EncodedDeploymentGraph(`{}`),
				Tofu:              model.RawTofu("terraform {}"),
			},
		)
		if admission == nil {
			admission = tx.Commit()
		}
	}()
	close(start)
	group.Wait()
	require.NoError(t, completion)
	if admission != nil {
		conflict, ok := model.IsErrConflict(admission)
		require.True(t, ok, "expected environment-busy refusal, got %v", admission)
		require.Equal(t, model.CodeEnvironmentBusy, conflict.Code)
		next = f.create(false)
	}
	f.requireEffective(next.Id)
}

func TestPipelineNoEffectCannotEraseKnownUnknownOutcome(t *testing.T) {
	f := newNoEffectFixture(t)
	failed, receipt := f.pipeline()
	require.NoError(t, f.progress(failed, "outcome_unknown", 1))
	require.NoError(t, f.progress(failed, "failed", 2), "later generic failure cannot erase retained uncertainty")
	body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
	require.ErrorIs(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, body), body), ErrRunnerEventTargetInvalid)
	f.requireEffective(failed.Id)
}

func TestPipelineUnknownOutcomeCannotContradictNoEffectReceipt(t *testing.T) {
	f := newNoEffectFixture(t)
	failed, receipt := f.pipeline()
	body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, body), body))
	require.ErrorIs(t, f.progress(failed, "outcome_unknown", 99), ErrRunnerEventTargetInvalid)
	require.NoError(t, f.progress(failed, "applying", 100), "a stale nonterminal intention is ignored, not treated as proof of an effect")
	f.requireEffective(uuid.Nil)
}

func TestPipelineNoEffectRefusesChangedAuthorityAndMutationEvidence(t *testing.T) {
	for _, change := range []string{"digest", "runner", "outputs", "metadata", "success", "applied", "legacy"} {
		t.Run(change, func(t *testing.T) {
			f := newNoEffectFixture(t)
			failed, receipt := f.pipeline()
			body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
			switch change {
			case "digest":
				receipt.CommandSha256 = strings.Repeat("0", 64)
			case "outputs":
				output := "Y2lwaGVydGV4dA=="
				body.Outputs = &output
			case "metadata":
				body.Metadata = DeploymentResultMetadata{{NodeId: "private"}}
			case "success":
				body.Status = Success
			case "applied", "legacy":
				tx, err := f.server.Database.BeginTx(f.ctx, nil)
				require.NoError(t, err)
				if change == "applied" {
					_, err = tx.ExecContext(f.ctx, `UPDATE deployment_pipeline_runs SET progress=jsonb_set(progress,'{infrastructure_applied}','true') WHERE deployment_id=$1`, failed.Id)
				} else {
					_, err = tx.ExecContext(f.ctx, `UPDATE deployment_pipeline_runs SET effect_base_captured=false WHERE deployment_id=$1`, failed.Id)
				}
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
			}
			event := f.event(failed, body)
			if change == "runner" {
				event.RunnerID = "other-site"
			}
			require.ErrorIs(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, event, body), ErrRunnerEventTargetInvalid)
			f.requireEffective(failed.Id)
			tx, err := f.server.Database.BeginTx(f.ctx, nil)
			require.NoError(t, err)
			var count int
			require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM pipeline_no_effect_receipts`).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, tx.Rollback())
		})
	}
}

func TestPipelineNoEffectPreservesPartialProviderHistoryBeforeAttemptBound(t *testing.T) {
	f := newNoEffectFixture(t)
	initial := f.create(false)
	_, err := f.server.Database.UpdateDeploymentStatusAndOutputs(f.ctx, nil, initial.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusSucceeded, RunnerCompletionProven: true})
	require.NoError(t, err)
	partial := f.create(false)
	_, err = f.server.Database.UpdateDeploymentStatusAndOutputs(f.ctx, nil, partial.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed, RunnerCompletionProven: true})
	require.NoError(t, err)
	var latest uuid.UUID
	for range maxProviderHistoryAttempts + 1 {
		failed, receipt := f.pipeline()
		body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
		require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, body), body))
		latest = failed.Id
	}
	f.requireEffective(partial.Id)
	head, _, tofu, graph, err := f.server.Database.GetLastDeployment(f.ctx, nil, partial.OrgId, partial.ProjectId, partial.EnvId, model.GetLastDeploymentParams{StateChangeOnly: true})
	require.NoError(t, err)
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	history, known, err := providerStateHistory(f.ctx, f.server.Database, tx, head, graphs.ProviderSnapshot{Graph: graph, Tofu: tofu})
	require.NoError(t, err)
	require.True(t, known, "proven no-effect attempts do not consume the state-bearing attempt bound")
	require.Len(t, history, 2)
	require.True(t, history[0].MayBePartial, "the partial failed attempt is retained")
	require.False(t, history[1].MayBePartial, "the prior successful boundary is retained")
	public, _, err := f.server.Database.ListDeployments(f.ctx, tx, partial.OrgId, "", maxProviderHistoryAttempts+3, model.ListDeploymentsParams{})
	require.NoError(t, err)
	require.Len(t, public, maxProviderHistoryAttempts+3, "public failed deployment history remains complete")
	require.Equal(t, latest, public[0].Id)
	require.Equal(t, partial.Id, public[len(public)-2].Id)
	require.Equal(t, initial.Id, public[len(public)-1].Id)
}

func TestPipelineNoEffectMissingTargetIsTerminalAndStorageErrorsRetry(t *testing.T) {
	f := newNoEffectFixture(t)
	failed, receipt := f.pipeline()
	body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
	missing := *failed
	missing.Id = uuid.New()
	err := f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(&missing, body), body)
	require.ErrorIs(t, err, ErrRunnerEventTargetInvalid)
	_, absent := model.IsErrNotFound(err)
	require.True(t, absent, "the HTTP boundary also retains its existing404 projection")
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(f.ctx, `ALTER TABLE deployment_pipeline_runs RENAME TO unavailable_pipeline_runs`)
	require.NoError(t, err)
	err = ingestPipelineNoEffect(f.ctx, tx, failed, &body, true)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrRunnerEventTargetInvalid, "unavailable storage remains retryable")
	require.NoError(t, tx.Rollback())
	cancelled, cancel := context.WithCancel(f.ctx)
	cancel()
	tx, err = f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	err = ingestPipelineNoEffect(cancelled, tx, failed, &body, true)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, ErrRunnerEventTargetInvalid)
}

func TestPipelineNoEffectCannotErasePriorResultEvidence(t *testing.T) {
	for _, evidence := range []string{"counts", "outputs", "metadata"} {
		t.Run(evidence, func(t *testing.T) {
			f := newNoEffectFixture(t)
			failed, receipt := f.pipeline()
			original := DeploymentResultsUpdateBody{Status: Failure}
			switch evidence {
			case "counts":
				original.TfResourceCounts.NumResourcesAdded = 1
			case "outputs":
				outputs := "Y2lwaGVydGV4dA=="
				original.Outputs = &outputs
			case "metadata":
				original.Metadata = DeploymentResultMetadata{{NodeId: "fixture-node", Metadata: map[string]any{"fixture": "observed"}}}
			}
			require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, original), original))
			qualified := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
			require.ErrorIs(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, qualified), qualified), ErrRunnerEventTargetInvalid, "same-event zero evidence cannot replace retained positive facts")
			f.requireEffective(failed.Id)
		})
	}
}

func TestPipelineResultEvidenceCannotContradictNoEffectReceipt(t *testing.T) {
	f := newNoEffectFixture(t)
	failed, receipt := f.pipeline()
	original := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
	require.NoError(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, original), original))
	conflicting := DeploymentResultsUpdateBody{Status: Failure, TfResourceCounts: DeploymentTFResourceCounts{NumResourcesAdded: 1}}
	require.ErrorIs(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, f.event(failed, conflicting), conflicting), ErrRunnerEventTargetInvalid)
	f.requireEffective(uuid.Nil)
}

type noEffectFixture struct {
	t           *testing.T
	ctx         context.Context
	server      *Server
	environment uuid.UUID
}

func newNoEffectFixture(t *testing.T) noEffectFixture {
	t.Helper()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	t.Cleanup(cancel)
	return noEffectFixture{t: t, ctx: ctx, server: &Server{Database: deploymentAdmissionTestDatabase(t, ctx, connection), Logger: zap.NewNop(), EventPublisher: &hmessaging.RecordingPublisher{}}, environment: uuid.New()}
}

func (f noEffectFixture) create(pipeline bool) *model.DeploymentSummary {
	f.t.Helper()
	params := model.CreateDeploymentParams{
		DeploymentEnvUuid: f.environment,
		Mode:              model.DeploymentModeDeploy,
		CreatedBy:         uuid.New(),
		RunnerId:          "site",
		RunnerLogLevel:    "info",
		Manifest:          model.EncodedDeploymentManifest(`{"workloads":{}}`),
		Graph:             model.EncodedDeploymentGraph(`{"nodes":{},"edges":{}}`),
		Tofu:              model.RawTofu("terraform {}"),
	}
	if pipeline {
		var err error
		params.Pipeline, err = model.ResolvePipeline(&model.PipelineSelection{ID: model.PipelineReviewed, Version: 1}, params.Mode)
		require.NoError(f.t, err)
	}
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	defer func() { _ = tx.Rollback() }()
	dep, err := f.server.Database.CreateDeployment(f.ctx, tx, "no-effect-org", "project", "env", params)
	require.NoError(f.t, err)
	require.NoError(f.t, tx.Commit())
	return dep
}

func (f noEffectFixture) pipeline() (*model.DeploymentSummary, *PipelineNoEffectReceipt) {
	f.t.Helper()
	dep := f.create(true)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(f.t, err)
	dispatcher := PipelineDispatcher{Server: f.server, Key: key}
	definition, err := model.ResolvePipeline(&model.PipelineSelection{ID: model.PipelineReviewed, Version: 1}, dep.Mode)
	require.NoError(f.t, err)
	raw, err := dispatcher.signCommand(dep, *definition, []byte("compiled fixture"), json.RawMessage(`{}`), "kubernetes", nil, nil, "")
	require.NoError(f.t, err)
	var envelope hmessaging.CommandEnvelope
	var signed struct {
		Payload json.RawMessage `json:"payload"`
	}
	require.NoError(f.t, json.Unmarshal(raw, &envelope))
	require.NoError(f.t, json.Unmarshal(envelope.Payload, &signed))
	sum := sha256.Sum256(signed.Payload)
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, dep.Id, raw)
	require.NoError(f.t, err)
	require.NoError(f.t, tx.Commit())
	return dep, &PipelineNoEffectReceipt{Version: StellwerkNoEffectv1, Effect: NotStarted, CommandSha256: hex.EncodeToString(sum[:])}
}

func (f noEffectFixture) event(dep *model.DeploymentSummary, body DeploymentResultsUpdateBody) hmessaging.EventEnvelope {
	f.t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(f.t, err)
	return hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, Type: "deployment-result", CreatedAt: time.Now().UTC(), EventID: dep.Id.String() + ":deployment-result", OrganizationID: dep.OrgId, RunnerID: dep.RunnerId, DeploymentID: dep.Id.String(), Payload: raw}
}

func (f noEffectFixture) progress(dep *model.DeploymentSummary, phase string, revision int64) error {
	f.t.Helper()
	raw, err := json.Marshal(map[string]any{"progress": model.PipelineProgress{ID: dep.Id.String(), EnvironmentID: f.environment.String(), Phase: phase, Revision: revision}})
	require.NoError(f.t, err)
	return f.server.ApplyPipelineProgressEnvelope(f.ctx, hmessaging.EventEnvelope{OrganizationID: dep.OrgId, RunnerID: dep.RunnerId, DeploymentID: dep.Id.String(), Payload: raw})
}

func (f noEffectFixture) requireEffective(expected uuid.UUID, message ...any) {
	f.t.Helper()
	dep, _, _, _, err := f.server.Database.GetLastDeployment(f.ctx, nil, "no-effect-org", "project", "env", model.GetLastDeploymentParams{StateChangeOnly: true})
	if expected == uuid.Nil {
		_, missing := model.IsErrNotFound(err)
		require.True(f.t, missing)
		return
	}
	require.NoError(f.t, err)
	require.Equal(f.t, expected, dep.Id, message...)
}
