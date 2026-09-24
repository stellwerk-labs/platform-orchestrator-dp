//go:build integration

package model

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

// This model test owns a unique schema and exercises the real migrations and
// database transaction path, without a CP, Runner, or infrastructure deployment.
func TestDeploymentIdempotencySerializesPreviouslyAbsentKey(t *testing.T) {
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection, "MODEL_TEST_DB_CONNECTION_STRING must name an isolated local PostgreSQL test database")
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "deployment_key_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	store, err := NewDatabaser(ctx, zap.NewNop(), connection+" search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	first, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = first.Rollback() }()
	const orgID, projectID, envID = "race-org", "race-project", "race-env"
	key := uuid.NewString()
	_, _, err = store.GetDeploymentByIdempotencyKeyDigest(ctx, first, orgID, projectID, envID, key)
	_, missing := IsErrNotFound(err)
	require.True(t, missing, "the first request owns a key with no deployment row yet")
	second, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() {
		_ = first.Rollback()
		_ = second.Rollback()
	}()
	var secondPID int
	require.NoError(t, second.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&secondPID))
	type result struct {
		deployment *DeploymentSummary
		err        error
	}
	replayed := make(chan result, 1)
	go func() {
		deployment, _, err := store.GetDeploymentByIdempotencyKeyDigest(ctx, second, orgID, projectID, envID, key)
		replayed <- result{deployment: deployment, err: err}
	}()
	// Observe a real PostgreSQL lock wait instead of relying on sleep timing.
	require.Eventually(t, func() bool {
		var waiting bool
		err := admin.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock' AND wait_event='advisory')`, secondPID).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	requestDigest := strings.Repeat("a", 64)
	created, err := store.CreateDeployment(ctx, first, orgID, projectID, envID, CreateDeploymentParams{
		CreatedBy: uuid.New(), DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy,
		Manifest: EncodedDeploymentManifest(`{"workloads":{}}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"),
		RunnerId: "unused-runner", RunnerLogLevel: "info", IdempotencyKeyDigest: opt.Of(key), ModuleVersionRequestDigest: requestDigest,
	})
	require.NoError(t, err)
	require.NoError(t, first.Commit())
	select {
	case outcome := <-replayed:
		require.NoError(t, outcome.err)
		require.Equal(t, created.Id, outcome.deployment.Id)
		require.Equal(t, requestDigest, outcome.deployment.ModuleVersionRequestDigest)
	case <-ctx.Done():
		t.Fatal("same-key waiter did not observe the committed original deployment")
	}
	require.NoError(t, second.Commit())
	loaded, _, _, _, err := store.GetDeployment(ctx, nil, orgID, created.Id, GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, requestDigest, loaded.ModuleVersionRequestDigest)
	var count int
	require.NoError(t, admin.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s.deployments", schema)).Scan(&count))
	require.Equal(t, 1, count)
}
