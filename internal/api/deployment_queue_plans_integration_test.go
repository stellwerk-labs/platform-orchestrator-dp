//go:build integration

package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

// Plans and dry runs never wait in the queue. A dry run answers while a
// Deployment runs; a plan-only run starts only while no deploy, rollback or
// destroy runs, because the Runner's plan takes the Environment's state lock.
func TestDeploymentQueueAnswersPlansAndDryRunsWithTheirPendingChanges(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	running := f.mustDeploy(queueBody(nil))
	f.mustQueue(f.deploy(queueBody(nil)))
	f.mustQueue(f.deploy(queueBody(map[string]any{"runner_log_level": "debug"})))

	whileRunning := map[string]any{"code": "result_may_change", "running": 1.0, "queued": 2.0}
	require.Equal(t, whileRunning, f.dryRunPendingChanges(nil))
	require.Equal(t, whileRunning, f.dryRunPendingChanges(map[string]any{"when_busy": "queue", "queue_ttl_seconds": 60}))
	require.Equal(t, whileRunning, f.dryRunPendingChanges(map[string]any{"manifest": nil, "mode": "rollback", "rollback_to_deployment_id": running.Id}))
	for _, body := range []map[string]any{{"plan_only": true}, {"mode": "plan_only", "when_busy": "queue"}} {
		f.requireError(f.deploy(queueBody(body)), http.StatusConflict, model.CodeEnvironmentBusy)
	}
	require.Len(t, f.queued(), 2, "plans and dry runs never wait")

	// The Deployment ended and Core still settles it: the head waits, nothing runs, a plan may start.
	f.complete(running.Id, model.DeploymentStatusSucceeded)
	f.promote(f.server)
	require.Len(t, f.queued(), 2)
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	require.True(t, plan.PlanOnly)
	require.Equal(t, &PendingEnvironmentChanges{Code: ResultMayChange, Running: 0, Queued: 2}, plan.PendingChanges)
	require.Equal(t, map[string]any{"code": "result_may_change", "running": 0.0, "queued": 2.0}, f.dryRunPendingChanges(nil))

	// The head waits behind the plan run, then the queue drains in order.
	f.settle(f.server, running.Id)
	require.Len(t, f.queued(), 2, "the queue waits for the plan run")
	f.complete(plan.Id, model.DeploymentStatusSucceeded)
	f.promote(f.server)
	require.Len(t, f.queued(), 1)
	deployments := f.deployments()
	f.finish(deployments[len(deployments)-1].Id, model.DeploymentStatusSucceeded)
	require.Empty(t, f.queued())
	deployments = f.deployments()
	f.finish(deployments[len(deployments)-1].Id, model.DeploymentStatusSucceeded)
	require.Len(t, deployments, 4)
	require.Nil(t, f.dryRunPendingChanges(nil), "nothing runs or waits")
	idlePlan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	require.Nil(t, idlePlan.PendingChanges)
}
