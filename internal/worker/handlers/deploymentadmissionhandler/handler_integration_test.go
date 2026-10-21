//go:build integration

package deploymentadmissionhandler

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

// A finished Deployment promotes its Environment's queue from the event that
// reports it, after Core settlement and before the remaining handlers run.
func TestFinishedDeploymentEventPromotesTheEnvironmentQueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	schema := "queue_trigger_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := admin.ExecContext(context.WithoutCancel(ctx), "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	})
	store, err := model.NewDatabaser(ctx, zap.NewNop(), connection+" search_path="+schema)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	environment := uuid.New()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	item, _, err := model.EnqueueDeployment(ctx, tx, model.NewDeploymentQueueItem{OrgID: "org", ProjectID: "project", EnvID: "environment", EnvironmentUUID: environment,
		Mode: model.DeploymentModeDeploy, Intent: json.RawMessage(`{}`), CreatedBy: uuid.New(), TTL: time.Hour}, 10)
	require.NoError(t, err)
	// An expired head is settled by promotion alone, which makes the trigger observable without Core.
	_, err = tx.ExecContext(ctx, `UPDATE environment_deployment_queue SET created_at=now()-interval '2 hours', expires_at=now()-interval '1 second' WHERE id=$1`, item.ID)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	nextCalls := 0
	handler := Handler{Queue: &api.Server{Database: store, Logger: zap.NewNop()}, Next: handlers.HandlerFunc(func(context.Context, *zap.Logger, hmessaging.Delivery) error {
		nextCalls++
		return nil
	})}
	deliver := func(status model.DeploymentStatus) {
		t.Helper()
		value := string(status)
		data, err := json.Marshal(events.CloudEvent[genevents.DeploymentChangedData]{Type: genevents.IoPlatformOrchestratorDeploymentUpdated,
			Data: genevents.DeploymentChangedData{DeploymentId: uuid.New(), EnvUuid: environment, OrgId: "org", ProjectId: "project", EnvId: "environment", Status: &value}})
		require.NoError(t, err)
		require.NoError(t, handler.Handle(ctx, zaptest.NewLogger(t), hmessaging.Delivery{Message: hmessaging.Message{Subject: string(genevents.IoPlatformOrchestratorDeploymentUpdated), Data: data}}))
	}
	state := func() model.DeploymentQueueState {
		t.Helper()
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		current, err := model.GetDeploymentQueueItem(ctx, tx, "org", item.ID)
		require.NoError(t, err)
		return current.State
	}

	deliver(model.DeploymentStatusExecuting)
	require.Equal(t, model.DeploymentQueueQueued, state(), "progress of a running Deployment frees nothing")
	deliver(model.DeploymentStatusFailed)
	require.Equal(t, model.DeploymentQueueExpired, state())
	require.Equal(t, 2, nextCalls)
}
