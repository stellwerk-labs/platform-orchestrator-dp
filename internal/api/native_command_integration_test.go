//go:build integration

package api

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"testing"

	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stretchr/testify/require"
)

func TestNativeNoEffectRefusesLegacyFrozenCommand(t *testing.T) {
	f := newNoEffectFixture(t)
	dep, receipt := f.pipeline()
	// Synthetic provenance is only a refusal fixture, never a positive v2 proof.
	target := manualNativeTarget(t)
	target.RunnerId = dep.RunnerId
	dep.NativeTarget = target
	raw, err := json.Marshal(target)
	require.NoError(t, err)
	tx, err := f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, dep.Id, raw)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	body := DeploymentResultsUpdateBody{Status: Failure, NoEffectReceipt: receipt}
	event := f.event(dep, body)
	event.RunnerID, err = hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	require.NoError(t, err)
	require.ErrorIs(t, f.server.ApplyRunnerDeploymentResultEnvelope(f.ctx, event, body), ErrRunnerEventTargetInvalid)
	tx, err = f.server.Database.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var receipts, inbox int
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM pipeline_no_effect_receipts WHERE deployment_id=$1`, dep.Id).Scan(&receipts))
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT count(*) FROM runner_event_inbox WHERE deployment_id=$1`, dep.Id).Scan(&inbox))
	require.Zero(t, receipts)
	require.Zero(t, inbox)
	f.requireEffective(dep.Id)
}

// This is decoder/capture continuity, not a claim of Core Native acceptance or
// artifact execution. The fixture's actual compiled backend remains Kubernetes.
func TestNativeSavedSourceDecoderKeepsSourceAndCaptureAuthority(t *testing.T) {
	f := nativePlanFixture(t)
	f.core.set(func(c *queueCore) { c.storageNamespace = "decoder-source" })
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	completeNativePlan(t, f, plan.Id)
	target := manualNativeTarget(t)
	// Match the actual compiled fixture backend, not the generic S3 helper default.
	backend, state, err := cp.StateStorageConfigurationDigest(json.RawMessage(`{"type":"kubernetes","namespace":"decoder-source"}`))
	require.NoError(t, err)
	target.StateBackendType, target.StateConfigurationDigest = backend, state
	setManualPlanTargets(t, f, plan.Id, target, target)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	source, err := f.server.loadPipelinePlan(f.ctx, tx, queueOrg, plan.Id, f.actor)
	require.NoError(t, err)
	require.Equal(t, "kubernetes", source.Backend, "never label a local fixture as registered Kubernetes")
	require.Contains(t, string(source.Tofu), "decoder-source")
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	dispatcher := PipelineDispatcher{Server: f.server, Key: key}
	raw, err := dispatcher.signCommand(source.Deployment, source.Run.Definition, []byte("scope-only retained fixture"), json.RawMessage(`{}`), source.Backend, &pipelinePlanBase{BaseDeploymentID: source.Capture.BaseDeploymentID.String()}, nil, "")
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_pipeline_runs SET command=$2 WHERE deployment_id=$1`, plan.Id, raw)
	require.NoError(t, err)
	child := *source.Deployment
	child.Id = uuid.New()
	reference := &pipelineSourcePlan{SourcePlanID: plan.Id.String(), Subject: source.Run.Progress.Subject, BaseDeploymentID: source.Capture.BaseDeploymentID.String()}
	decoded, err := loadSavedPlanCommand(f.ctx, tx, f.store, &child, reference)
	require.NoError(t, err)
	require.Equal(t, target, decoded.NativeTarget)
	changed := *target
	changed.StateConfigurationDigest = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	altered, err := json.Marshal(changed)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, plan.Id, altered)
	require.NoError(t, err)
	_, err = loadSavedPlanCommand(f.ctx, tx, f.store, &child, reference)
	require.ErrorContains(t, err, "target changed")
	original, err := json.Marshal(target)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployments SET native_target=$2 WHERE id=$1`, plan.Id, original)
	require.NoError(t, err)
	_, err = tx.ExecContext(f.ctx, `UPDATE deployment_pipeline_runs SET plan_capture=jsonb_set(plan_capture,'{native_target}',$2::jsonb) WHERE deployment_id=$1`, plan.Id, altered)
	require.NoError(t, err)
	_, err = loadSavedPlanCommand(f.ctx, tx, f.store, &child, reference)
	require.ErrorContains(t, err, "target changed")
}
