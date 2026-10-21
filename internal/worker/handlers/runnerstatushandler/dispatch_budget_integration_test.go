//go:build integration

package runnerstatushandler

import (
	"context"
	"database/sql"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

// This regression also runs against the old handler: its admission-clock
// timeout fails the Deployment before even attempting the real Core transport.
func TestOldAdmissionDoesNotTimeoutFreshDurableRunnerDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	connection := os.Getenv("MODEL_TEST_DB_CONNECTION_STRING")
	require.NotEmpty(t, connection)
	admin, err := sql.Open("postgres", connection)
	require.NoError(t, err)
	defer func() { require.NoError(t, admin.Close()) }()
	schema := "dispatch_watchdog_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		_, err := admin.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE")
		require.NoError(t, err)
	}()
	store, err := model.NewDatabaser(ctx, zap.NewNop(), connection+" search_path="+schema)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	dep, err := store.CreateDeployment(
		ctx,
		tx,
		testOrgID,
		"p",
		"e",
		model.CreateDeploymentParams{
			CreatedBy:         uuid.New(),
			DeploymentEnvUuid: uuid.New(),
			Mode:              model.DeploymentModeDeploy,
			RunnerId:          testRunnerID,
			RunnerLogLevel:    "info",
			Manifest:          model.EncodedDeploymentManifest(`{}`),
			Graph:             model.EncodedDeploymentGraph(`{}`),
			Tofu:              model.RawTofu("terraform {}"),
		},
	)
	require.NoError(t, err)
	// The same explicit durable fixture is available to both old and new code.
	// CREATE IF NOT EXISTS adds only the unconsumed test evidence on schema30.
	_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS deployment_dispatch_budgets(deployment_id UUID PRIMARY KEY,deployment_revision INTEGER NOT NULL,first_attempt_at TIMESTAMPTZ,deadline TIMESTAMPTZ,completion_received_at TIMESTAMPTZ,legacy BOOLEAN NOT NULL DEFAULT false)`)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `INSERT INTO deployment_dispatch_budgets(deployment_id,deployment_revision,first_attempt_at,deadline) VALUES($1,1,now(),now()+interval '1 hour')
        ON CONFLICT(deployment_id) DO UPDATE SET first_attempt_at=EXCLUDED.first_attempt_at,deadline=EXCLUDED.deadline`, dep.Id)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployments SET created_at=now()-interval '8 hours' WHERE id=$1`, dep.Id)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	// An unavailable real Core transport produces an ordinary retry, not an
	// invented response. The timeout decision must precede that transport.
	client, err := platformorchestratorcp.NewClientWithResponses("http://127.0.0.1:1", platformorchestratorcp.WithHTTPClient(&http.Client{Timeout: 100 * time.Millisecond}))
	require.NoError(t, err)
	handler := New(store, &hmessaging.RecordingPublisher{}, client, nil)
	handlerErr := handler.Handle(ctx, zap.NewNop(), statusDelivery(t, dep.Id))
	current, _, _, _, err := store.GetDeployment(ctx, nil, testOrgID, dep.Id, model.GetModeDefault)
	require.NoError(t, err)
	require.Equal(t, model.DeploymentStatusExecuting, current.Status, "old CreatedAt-based watchdog incorrectly finalized this fresh dispatch")
	require.Error(t, handlerErr)
	_, retry := hmessaging.AsRetryError(handlerErr)
	require.True(t, retry)
}
