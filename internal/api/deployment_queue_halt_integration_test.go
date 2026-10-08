//go:build integration

package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func (f *queueFixture) requireHalted(id, predecessor uuid.UUID) {
	f.t.Helper()
	item := f.item(id)
	require.Equal(f.t, model.DeploymentQueueRejected, item.State)
	require.Equal(f.t, opt.Of(model.QueueReasonPreviousDeploymentFailed), item.ReasonCode)
	require.Contains(f.t, item.ReasonMessage.Must(), predecessor.String(), "the reason names the failed Deployment")
	require.False(f.t, item.DeploymentID.IsSet())
}

func TestDeploymentQueueHaltsAnItemWhoseAwaitedDeploymentFailed(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	path := "/orgs/" + queueOrg + "/deployments"
	halt := map[string]any{"halt_on_failure": true}
	f.requireError(f.deploy(queueBody(map[string]any{"when_busy": "reject", "halt_on_failure": true})), http.StatusBadRequest, "HTTP-400")
	running := f.mustDeploy(queueBody(halt))

	halted := f.mustQueue(f.call(http.MethodPost, path, queueBody(halt), f.actor, "halt-1"))
	require.True(t, halted.HaltOnFailure)
	require.Equal(t, halted.Id, f.mustQueue(f.call(http.MethodPost, path, queueBody(halt), f.actor, "halt-1")).Id, "an identical replay returns the item")
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, queueBody(nil), f.actor, "halt-1").Code, "halt_on_failure is part of the request")
	continues := f.mustQueue(f.deploy(queueBody(map[string]any{"runner_log_level": "debug"})))
	superseded := f.mustQueue(f.deploy(queueBody(map[string]any{"halt_on_failure": true, "runner_log_level": "warn"})))

	f.finish(running.Id, model.DeploymentStatusFailed)
	f.requireHalted(halted.Id, running.Id)
	next := f.item(continues.Id)
	require.Equal(t, model.DeploymentQueueDispatched, next.State, "the queue continues after a halt")
	require.Equal(t, model.DeploymentQueueQueued, f.item(superseded.Id).State, "it waits for the Deployment ahead of it")

	// The Deployment it waited for succeeded; the earlier failure is superseded.
	f.finish(next.DeploymentID.Must(), model.DeploymentStatusSucceeded)
	dispatched := f.item(superseded.Id)
	require.Equal(t, model.DeploymentQueueDispatched, dispatched.State)

	// One failure halts every halt_on_failure item that waited for it; the others proceed.
	first := f.mustQueue(f.deploy(queueBody(halt)))
	second := f.mustQueue(f.deploy(queueBody(map[string]any{"halt_on_failure": true, "runner_log_level": "debug"})))
	plain := f.mustQueue(f.deploy(queueBody(map[string]any{"runner_log_level": "error"})))
	f.finish(dispatched.DeploymentID.Must(), model.DeploymentStatusFailed)
	f.requireHalted(first.Id, dispatched.DeploymentID.Must())
	f.requireHalted(second.Id, dispatched.DeploymentID.Must())
	require.Equal(t, model.DeploymentQueueDispatched, f.item(plain.Id).State)

	get := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployment-queue/"+first.Id.String(), nil, f.actor, "")
	require.Equal(t, http.StatusOK, get.Code, get.Body.String())
	var detail DeploymentQueueItem
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &detail))
	require.True(t, detail.HaltOnFailure)
	require.Equal(t, Rejected, detail.State)
	require.Equal(t, model.QueueReasonPreviousDeploymentFailed, *detail.ReasonCode)
	require.Len(t, f.deployments(), 4)
	require.Equal(t, len(f.deployments()), f.core.admissions(), "halted items never reach Core")
	require.Empty(t, f.queued())
}

func TestDeploymentQueueHaltCountsOnlyTheDeploymentTheItemWaitedFor(t *testing.T) {
	halt := map[string]any{"halt_on_failure": true}

	t.Run("a failure that ended before the item was queued", func(t *testing.T) {
		f := newQueueFixture(t, DeploymentQueueConfig{})
		failed := f.mustDeploy(queueBody(nil))
		f.complete(failed.Id, model.DeploymentStatusFailed)
		// The Data Plane is idle and Core still settles the failure: the request waits, but not for that failure.
		item := f.mustQueue(f.deploy(queueBody(halt)))
		require.Equal(t, opt.Of(failed.Id), f.item(item.Id).EnqueuedAfterDeploymentID)
		f.settle(f.server, failed.Id)
		require.Equal(t, model.DeploymentQueueDispatched, f.item(item.Id).State)
	})

	t.Run("plan runs never count", func(t *testing.T) {
		f := newQueueFixture(t, DeploymentQueueConfig{})
		// On a fresh Environment the first plan run is also its recorded state head.
		plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
		item := f.mustQueue(f.deploy(queueBody(halt)))
		require.False(t, f.item(item.Id).EnqueuedAfterDeploymentID.IsSet(), "the plan was still running")
		f.complete(plan.Id, model.DeploymentStatusFailed)
		f.promote(f.server)
		require.Equal(t, model.DeploymentQueueDispatched, f.item(item.Id).State)
	})

	t.Run("an unreconciled failure fences the Environment instead", func(t *testing.T) {
		f := newQueueFixture(t, DeploymentQueueConfig{})
		plugin := f.mustDeploy(queueBody(nil))
		f.markPluginDispatched(plugin.Id)
		item := f.mustQueue(f.deploy(queueBody(halt)))
		_, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, plugin.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed, StatusMessage: "watchdog timeout"})
		require.NoError(t, err)
		f.settle(f.server, plugin.Id)
		rejected := f.item(item.Id)
		require.Equal(t, model.DeploymentQueueRejected, rejected.State)
		require.Equal(t, opt.Of(model.CodeOutcomeUnreconciled), rejected.ReasonCode)
	})
}
