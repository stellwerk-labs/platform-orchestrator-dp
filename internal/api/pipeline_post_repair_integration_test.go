//go:build integration

package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
	"github.com/stretchr/testify/require"
)

func TestNativePostRepairProgressPreservesDelayedMetadataWithoutReceiptOrdering(t *testing.T) {
	f, dep, _ := nativeControlFixture(t)
	var command []byte
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	require.NoError(t, tx.QueryRowContext(f.ctx, `SELECT command FROM deployment_pipeline_runs WHERE deployment_id=$1`, dep.Id).Scan(&command))
	require.NoError(t, tx.Rollback())
	accepted, err := model.FrozenPipelinePayload(command, dep)
	require.NoError(t, err)
	target := dep.NativeTarget
	capture := &nativelogs.Descriptor{Version: 1, Status: nativelogs.NotManaged, Authority: nativelogs.Authority{
		OrganizationID: dep.OrgId, EnvironmentID: dep.DeploymentEnvUuid.String(), DeploymentID: dep.Id.String(),
		CommandDigest: nativelogs.Hash(accepted), Phase: nativelogs.ApplyPhase, ExecutionID: dep.Id.String(), CaptureRevision: 3,
		Target: nativelogs.Target{RunnerID: target.RunnerId, RunnerUUID: target.RunnerUuid.String(),
			RuntimeVersion: string(target.RuntimeVersion), PipelineVersion: string(target.PipelineVersion),
			ToolsDigest: target.ToolsDigest, StateBackendType: string(target.StateBackendType), StateConfigurationDigest: target.StateConfigurationDigest},
	}}
	grant := &model.PipelinePostRepairAuthority{ControlID: uuid.NewString(), ExpiresAt: time.Now().UTC().Add(-time.Hour)}
	progress := model.PipelineProgress{ID: dep.Id.String(), EnvironmentID: dep.DeploymentEnvUuid.String(), Phase: "post_failed", Revision: 4, InfrastructureApplied: true, FailedStep: "post", PostRepair: grant, LogCapture: capture}
	payload, err := json.Marshal(map[string]any{"progress": progress})
	require.NoError(t, err)
	route, err := hmessaging.NativeRunnerToken(dep.NativeTarget.RunnerUuid.String())
	require.NoError(t, err)
	envelope := hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, EventID: dep.Id.String() + ":4", OrganizationID: dep.OrgId, RunnerID: route, DeploymentID: dep.Id.String(), Type: "rollout-progress", CreatedAt: time.Now().UTC(), Payload: payload}
	require.NoError(t, f.server.ApplyPipelineProgressEnvelope(f.ctx, envelope))
	require.NoError(t, f.server.ApplyPipelineProgressEnvelope(f.ctx, envelope))
	run, err := f.server.readDeploymentPipeline(f.ctx, dep.Id)
	require.NoError(t, err)
	require.Equal(t, grant, run.Progress.PostRepair)
	require.Equal(t, capture, run.Progress.LogCapture)
	require.Zero(t, f.count(`SELECT count(*) FROM deployment_pipeline_controls`), "progress cannot require the independently delivered control result to arrive first")
	require.Equal(t, "post_failed", run.Progress.Phase)
	f.exec(`UPDATE deployments SET native_target=NULL WHERE id=$1`, dep.Id)
	envelope.RunnerID = dep.RunnerId
	require.ErrorIs(t, f.server.ApplyPipelineProgressEnvelope(f.ctx, envelope), ErrRunnerEventTargetInvalid)
}
