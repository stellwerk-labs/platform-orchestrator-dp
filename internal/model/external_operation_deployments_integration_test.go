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

func TestExternalOperationDeploymentReceiptSerializesAndSurvivesDeploymentRetention(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	admin, _, scopedConnection := migrationTestDatabase(t, ctx, connection)
	store, err := NewDatabaser(ctx, zap.NewNop(), scopedConnection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	key := ExternalOperationDeploymentKey{
		OrgID:     "receipt-org",
		PluginID:  "dev.stellwerk.progressive-rollouts",
		RequestID: uuid.NewString(),
	}
	operationRef := uuid.NewString()
	first, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = first.Rollback() }()
	_, err = GetExternalOperationDeploymentReceipt(ctx, first, key)
	_, missing := IsErrNotFound(err)
	require.True(t, missing)

	second, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = second.Rollback() }()
	var secondPID int
	require.NoError(t, second.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&secondPID))
	type lookupResult struct {
		receipt *ExternalOperationDeploymentReceipt
		err     error
	}
	waiter := make(chan lookupResult, 1)
	go func() {
		receipt, err := GetExternalOperationDeploymentReceipt(ctx, second, key)
		waiter <- lookupResult{receipt: receipt, err: err}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND wait_event='advisory')`, secondPID).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)

	actorID := uuid.New()
	projectUUID, environmentUUID, snapshotID, baseDeploymentID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	deployment, err := store.CreateDeployment(ctx, first, key.OrgID, "receipt-project", "receipt-env", CreateDeploymentParams{
		CreatedBy: actorID, DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy,
		Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"),
		RunnerId: "unused-runner", RunnerLogLevel: "info", IdempotencyKeyDigest: opt.Empty[string](),
	})
	require.NoError(t, err)
	expected, err := CreateExternalOperationDeploymentReceipt(ctx, first, ExternalOperationDeploymentReceipt{
		ExternalOperationDeploymentKey: key,
		OperationRef:                   operationRef,
		Fence:                          7,
		AssertionAction:                "external-deployment.admit",
		ServiceAction:                  "external-deployment.admit",
		RequestBodySHA256:              strings.Repeat("a", 64),
		ActorID:                        actorID.String(),
		ProjectID:                      deployment.ProjectId,
		EnvID:                          deployment.EnvId,
		ProjectUUID:                    opt.Of(projectUUID),
		EnvironmentUUID:                opt.Of(environmentUUID),
		PlanSnapshotID:                 opt.Of(snapshotID),
		BaseDeploymentID:               opt.Of(baseDeploymentID),
		DeploymentID:                   deployment.Id,
	})
	require.NoError(t, err)
	require.NoError(t, first.Commit())

	select {
	case result := <-waiter:
		require.NoError(t, result.err)
		require.Equal(t, expected.DeploymentID, result.receipt.DeploymentID)
		require.Equal(t, expected.Fence, result.receipt.Fence)
		require.Equal(t, snapshotID, result.receipt.PlanSnapshotID.Must())
		require.Equal(t, baseDeploymentID, result.receipt.BaseDeploymentID.Must())
	case <-ctx.Done():
		t.Fatal("same-request waiter did not observe the committed receipt")
	}
	require.NoError(t, second.Commit())

	deletion, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteDeploymentsForEnv(ctx, deletion, key.OrgID, deployment.ProjectId, deployment.EnvId, true))
	require.NoError(t, deletion.Commit())
	retained, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = retained.Rollback() }()
	var receipts int
	require.NoError(t, retained.QueryRowContext(ctx,
		`SELECT count(*) FROM external_operation_deployments WHERE deployment_id=$1`, deployment.Id,
	).Scan(&receipts))
	require.Equal(t, 1, receipts, "normal deployment retention must not erase the durable operation receipt")
	require.NoError(t, retained.Commit())
}
