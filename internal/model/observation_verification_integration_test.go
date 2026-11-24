//go:build integration

package model

import (
	"github.com/google/uuid"
	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVerificationObservationFreezesExactExpectedHead(t *testing.T) {
	f, base := observationBaselineFixture(t)
	request := observationRequest(f)
	_, manifest, tofu, graph, err := f.store.GetDeployment(f.ctx, nil, "queue-org", base.Id, GetModeDefault)
	require.NoError(t, err)
	fingerprint := ObservationBaselineFingerprint(manifest, tofu, graph, base.ModuleArtifacts, base.NativeTarget)
	request.ExpectedBaselineId = &base.Id
	request.ExpectedBaselineFingerprint = &fingerprint
	wrongID := uuid.New()
	request.ExpectedBaselineId = &wrongID
	receipt, pending, err := TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), request, "runner", 2, true, nil)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptReason("baseline_changed"), receipt.Reason)
	require.Empty(t, pending)
	require.Zero(t, readObservationCount(t, f, `SELECT count(*) FROM runner_job_reservations WHERE released_at IS NULL`))
	request.RequestId = uuid.New()
	request.ExpectedBaselineId = &base.Id
	receipt, pending, err = TryStartObservation(f.ctx, f.store, "queue-org", "dev.example.plugin", uuid.New(), request, "runner", 2, true, nil)
	require.NoError(t, err)
	require.Equal(t, observation.ObservationReceiptDispositionAccepted, receipt.Disposition)
	require.Len(t, pending, 1)
	dep, _, _, _, err := f.store.GetDeployment(f.ctx, nil, "queue-org", *receipt.RunId, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, DeploymentModeObservation, dep.Mode)
	require.False(t, dep.HasPipeline, "verification cannot execute mutating pipeline hooks")
}
