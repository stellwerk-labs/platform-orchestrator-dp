//go:build integration

package model

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestDeploymentAdmissionJournalSerializesOutcomeAndRetainsReceipts(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	admin, _, scopedConnection := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scopedConnection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	intent := testDeploymentAdmissionIntent()
	prepare, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	prepared, err := PrepareDeploymentAdmissionReceipt(ctx, prepare, intent)
	require.NoError(t, err)
	require.Equal(t, "pending", prepared.Disposition)
	require.NoError(t, prepare.Commit())

	live, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = live.Rollback() }()
	locked, err := GetDeploymentAdmissionReceipt(ctx, live, intent.OrgID, intent.AdmissionID)
	require.NoError(t, err)
	require.Equal(t, "pending", locked.Disposition)

	reconciler, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = reconciler.Rollback() }()
	var reconcilerPID int
	require.NoError(t, reconciler.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&reconcilerPID))
	type sealResult struct {
		receipt *DeploymentAdmissionReceipt
		err     error
	}
	sealed := make(chan sealResult, 1)
	go func() {
		receipt, err := SealDeploymentAdmissionAbsent(ctx, reconciler, intent, 0)
		sealed <- sealResult{receipt: receipt, err: err}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND wait_event='advisory')`, reconcilerPID).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)

	deployment, err := store.CreateDeployment(ctx, live, intent.OrgID, "project", "environment", CreateDeploymentParams{
		CreatedBy: intent.ActorID, DeploymentEnvUuid: intent.EnvironmentUUID, Mode: DeploymentModeDeploy,
		Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"),
		RunnerId: "runner", RunnerLogLevel: "info", IdempotencyKeyDigest: opt.Empty[string](),
	})
	require.NoError(t, err)
	bound, err := BindDeploymentAdmissionReceipt(ctx, live, intent, deployment.Id, 11)
	require.NoError(t, err)
	require.Equal(t, "committed", bound.Disposition)
	require.NoError(t, live.Commit())

	select {
	case result := <-sealed:
		_, conflict := IsErrConflict(result.err)
		require.True(t, conflict, "crash reconciler must not overwrite a committed Deployment: %v", result.err)
	case <-ctx.Done():
		t.Fatal("reconciler did not drain after the live admission committed")
	}
	require.NoError(t, reconciler.Rollback())

	settlement, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, MarkDeploymentAdmissionCoreSettled(ctx, settlement, intent.OrgID, intent.AdmissionID, "succeeded", 11))
	require.NoError(t, settlement.Commit())

	changed := intent
	changed.RequestDigest = strings.Repeat("b", 64)
	conflicting, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = PrepareDeploymentAdmissionReceipt(ctx, conflicting, changed)
	_, conflict := IsErrConflict(err)
	require.True(t, conflict)
	require.NoError(t, conflicting.Rollback())

	absentIntent := testDeploymentAdmissionIntent()
	absentPrepare, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = PrepareDeploymentAdmissionReceipt(ctx, absentPrepare, absentIntent)
	require.NoError(t, err)
	require.NoError(t, absentPrepare.Commit())
	absentTx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	absent, err := SealDeploymentAdmissionAbsent(ctx, absentTx, absentIntent, 0)
	require.NoError(t, err)
	require.Equal(t, "absent", absent.Disposition)
	require.Zero(t, absent.CoreEpoch)
	require.NoError(t, absentTx.Commit())
	absentSettled, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, MarkDeploymentAdmissionCoreSettled(ctx, absentSettled, absentIntent.OrgID, absentIntent.AdmissionID, "absent", 12))
	require.NoError(t, absentSettled.Commit())

	deletion, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteDeploymentsForEnv(ctx, deletion, intent.OrgID, deployment.ProjectId, deployment.EnvId, true))
	require.NoError(t, deletion.Commit())
	retained, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = retained.Rollback() }()
	var receipts int
	require.NoError(t, retained.QueryRowContext(ctx,
		`SELECT count(*) FROM deployment_admission_receipts WHERE admission_id IN ($1,$2)`, intent.AdmissionID, absentIntent.AdmissionID,
	).Scan(&receipts))
	require.Equal(t, 2, receipts, "admission outcomes must outlive normal Deployment retention")
	require.NoError(t, retained.Commit())
}

func testDeploymentAdmissionIntent() DeploymentAdmissionReceipt {
	module, version := uuid.New(), uuid.New()
	return DeploymentAdmissionReceipt{
		OrgID: "admission-org", AdmissionID: uuid.New(), ProjectUUID: uuid.New(), EnvironmentUUID: uuid.New(), ActorID: uuid.New(),
		RequestDigest: strings.Repeat("a", 64), Mode: DeploymentModeDeploy,
		ModuleVersions: map[string]uuid.UUID{module.String(): version}, Disposition: "pending",
	}
}
