//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hecho"
	coreapi "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

var queueEverything = map[string]any{"when_busy": "queue"}

func TestDeploymentQueueDispatchesInOrderWithPositions(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	running := f.mustDeploy(queueBody(nil))
	items := make([]DeploymentQueueItem, 3)
	for index := range items {
		items[index] = f.mustQueue(f.deploy(queueBody(map[string]any{"when_busy": "queue", "runner_log_level": []string{"debug", "warn", "error"}[index]})))
		require.Equal(t, Queued, items[index].State)
		require.Equal(t, index+1, items[index].Position)
		require.Equal(t, f.actor, items[index].CreatedBy)
		require.True(t, items[index].ExpiresAt.After(time.Now().Add(119*time.Minute)), "default TTL is two hours")
	}
	listed := f.call(http.MethodGet, "/orgs/"+queueOrg+"/deployment-queue?project_id="+queueProject+"&env_id="+queueEnv+"&state=queued", nil, f.actor, "")
	require.Equal(t, http.StatusOK, listed.Code, listed.Body.String())
	var page DeploymentQueueItemPage
	require.NoError(t, json.Unmarshal(listed.Body.Bytes(), &page))
	require.Len(t, page.Items, 3)
	for index, item := range page.Items {
		require.Equal(t, items[index].Id, item.Id)
		require.Equal(t, index+1, item.Position)
		require.Nil(t, item.Manifest, "lists never carry manifests")
	}

	dispatched := []uuid.UUID{running.Id}
	for index := range items {
		f.finish(dispatched[len(dispatched)-1], model.DeploymentStatusSucceeded)
		item := f.item(items[index].Id)
		require.Equal(t, model.DeploymentQueueDispatched, item.State)
		require.True(t, item.DeploymentID.IsSet())
		dispatched = append(dispatched, item.DeploymentID.Must())
		for later := index + 1; later < len(items); later++ {
			require.Equal(t, later-index, f.item(items[later].Id).Position, "positions move up as the head dispatches")
		}
	}
	deployments := f.deployments()
	require.Len(t, deployments, 4)
	for index, deployment := range deployments {
		require.Equal(t, dispatched[index], deployment.Id, "dispatch order is FIFO")
		require.Equal(t, f.actor, deployment.CreatedBy)
	}
	for index, level := range []string{"debug", "warn", "error"} {
		require.Equal(t, level, f.runnerLogLevel(dispatched[index+1]), "each Deployment resolves its own stored intent")
	}
	require.Empty(t, f.queued())
}

func (f *queueFixture) runnerLogLevel(deploymentID uuid.UUID) string {
	f.t.Helper()
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	defer func() { _ = tx.Rollback() }()
	var level string
	require.NoError(f.t, tx.QueryRowContext(f.ctx, `SELECT runner_log_level FROM deployments WHERE id=$1`, deploymentID).Scan(&level))
	return level
}

func TestDeploymentQueueIdleEnvironmentCreatesTheDeploymentAtOnce(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	response := f.deploy(queueBody(queueEverything))
	require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
	var deployment Deployment
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &deployment))
	require.Equal(t, "executing", deployment.Status)
	require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_queue`))
}

// Queueing is the default; an organization token's default request and an
// explicit reject still fail fast.
func TestDeploymentQueueDefaultRequestStillRejectsABusyEnvironment(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	rejectEverything := map[string]any{"when_busy": "reject"}
	organizationToken := func(body map[string]any) *http.Request {
		request := f.request(http.MethodPost, "/orgs/"+queueOrg+"/deployments", body, f.actor, "")
		request.Header.Set("Po-Org-Id", queueOrg)
		return request
	}
	running := f.mustDeploy(queueBody(nil))
	f.requireError(f.serve(organizationToken(queueBody(nil))), http.StatusConflict, model.CodeEnvironmentBusy)
	f.requireError(f.deploy(queueBody(rejectEverything)), http.StatusConflict, model.CodeEnvironmentBusy)
	f.requireError(f.deploy(queueBody(map[string]any{"when_busy": "reject", "manifest": nil, "mode": "rollback", "rollback_to_deployment_id": running.Id})),
		http.StatusConflict, model.CodeEnvironmentBusy)

	// Lease released, but Core still guards the finished admission until settlement.
	f.complete(running.Id, model.DeploymentStatusSucceeded)
	f.requireError(f.deploy(queueBody(rejectEverything)), http.StatusConflict, model.CodeEnvironmentBusy)
	f.requireError(f.serve(organizationToken(queueBody(nil))), http.StatusConflict, model.CodeEnvironmentBusy)
	require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_queue`))
	f.settle(f.server, running.Id)
	f.mustDeploy(queueBody(rejectEverything))
	require.Len(t, f.core.unsettledGuards(), 1, "refused attempts sealed their admissions")
}

func TestDeploymentQueueIsTheDefaultForABusyEnvironment(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	running := f.mustDeploy(queueBody(nil))
	deploy := f.mustQueue(f.deploy(queueBody(nil)))
	rollback := f.mustQueue(f.deploy(queueBody(map[string]any{"manifest": nil, "mode": "rollback", "rollback_to_deployment_id": running.Id})))
	require.Equal(t, []int{1, 2}, []int{deploy.Position, rollback.Position})
	require.False(t, deploy.HaltOnFailure, "halt_on_failure defaults to false")
	// Request validation fills in the published default; the handler applies the same one without it.
	direct, err := f.server.CreateDeployment(context.WithValue(f.ctx, hecho.ContextKeyUserID, f.actor.String()), CreateDeploymentRequestObject{
		OrgId: queueOrg, Body: &CreateDeploymentJSONRequestBody{ProjectId: queueProject, EnvId: queueEnv, Mode: DeploymentCreateBodyModeDeploy,
			Manifest: &DeploymentManifest{Workloads: map[string]DeploymentManifestWorkload{}}, RunnerLogLevel: ref.Ref(DeploymentCreateBodyRunnerLogLevelWarn)},
	})
	require.NoError(t, err)
	require.IsType(t, CreateDeployment202JSONResponse{}, direct)
	f.server.WaitDeploymentQueueKicks()
	require.Len(t, f.queued(), 3)
	cancelled := f.call(http.MethodPost, "/orgs/"+queueOrg+"/deployment-queue/"+direct.(CreateDeployment202JSONResponse).Id.String()+"/actions/cancel", nil, f.actor, "")
	require.Equal(t, http.StatusOK, cancelled.Code, cancelled.Body.String())
	require.Len(t, f.queued(), 2)

	f.finish(running.Id, model.DeploymentStatusSucceeded)
	first := f.item(deploy.Id)
	require.Equal(t, model.DeploymentQueueDispatched, first.State)
	f.finish(first.DeploymentID.Must(), model.DeploymentStatusSucceeded)
	rolledBack := f.item(rollback.Id)
	require.Equal(t, model.DeploymentQueueDispatched, rolledBack.State)
	require.Empty(t, f.queued())

	// The Data Plane is idle, Core is not: the direct attempt meets Core's guard and the default request waits.
	f.complete(rolledBack.DeploymentID.Must(), model.DeploymentStatusSucceeded)
	settling := f.mustQueue(f.deploy(queueBody(map[string]any{"runner_log_level": "debug"})))
	require.Equal(t, 1, settling.Position)
	f.settle(f.server, rolledBack.DeploymentID.Must())
	settled := f.item(settling.Id)
	require.Equal(t, model.DeploymentQueueDispatched, settled.State)
	require.Len(t, f.deployments(), 4)
	require.Empty(t, f.queued())

	f.finish(settled.DeploymentID.Must(), model.DeploymentStatusSucceeded)
	idle := f.mustDeploy(queueBody(nil))
	require.Nil(t, idle.PendingChanges, "only plans and dry runs report pending changes")
}

func TestDeploymentQueueReplaysIdempotentRequests(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	running := f.mustDeploy(queueBody(nil))
	path := "/orgs/" + queueOrg + "/deployments"
	first := f.mustQueue(f.call(http.MethodPost, path, queueBody(queueEverything), f.actor, "release-42"))
	replay := f.mustQueue(f.call(http.MethodPost, path, queueBody(queueEverything), f.actor, "release-42"))
	require.Equal(t, first.Id, replay.Id)
	require.Equal(t, first.Position, replay.Position)
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, queueBody(map[string]any{"when_busy": "queue", "runner_log_level": "debug"}), f.actor, "release-42").Code)
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, queueBody(map[string]any{"when_busy": "queue", "manifest": nil, "mode": "rollback", "rollback_to_deployment_id": running.Id}), f.actor, "release-42").Code)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM environment_deployment_queue`))

	f.finish(running.Id, model.DeploymentStatusSucceeded)
	item := f.item(first.Id)
	require.Equal(t, model.DeploymentQueueDispatched, item.State)
	for _, whenBusy := range []string{"queue", "reject"} {
		response := f.call(http.MethodPost, path, queueBody(map[string]any{"when_busy": whenBusy}), f.actor, "release-42")
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
		var deployment Deployment
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &deployment))
		require.Equal(t, item.DeploymentID.Must(), deployment.Id, "after dispatch the Deployment answers the replay")
	}
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, queueBody(map[string]any{"when_busy": "queue", "runner_log_level": "debug"}), f.actor, "release-42").Code)
	require.Len(t, f.deployments(), 2)

	// A Deployment created at once answers its replays while it runs and keeps the Environment busy.
	f.finish(item.DeploymentID.Must(), model.DeploymentStatusSucceeded)
	direct := f.call(http.MethodPost, path, queueBody(map[string]any{"runner_log_level": "warn"}), f.actor, "hotfix-7")
	require.Equal(t, http.StatusCreated, direct.Code, direct.Body.String())
	var created Deployment
	require.NoError(t, json.Unmarshal(direct.Body.Bytes(), &created))
	for _, whenBusy := range []any{nil, "queue", "reject"} {
		response := f.call(http.MethodPost, path, queueBody(map[string]any{"runner_log_level": "warn", "when_busy": whenBusy}), f.actor, "hotfix-7")
		require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
		var replayed Deployment
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &replayed))
		require.Equal(t, created.Id, replayed.Id, "the running Deployment answers, nothing is queued")
	}
	changed := queueBody(map[string]any{"manifest": map[string]any{"workloads": map[string]any{"api": map[string]any{}}}})
	require.Equal(t, http.StatusConflict, f.call(http.MethodPost, path, changed, f.actor, "hotfix-7").Code)
	require.Equal(t, 1, f.count(`SELECT count(*) FROM environment_deployment_queue`))
	require.Len(t, f.deployments(), 3)
}

func TestDeploymentQueueBoundsDepthAndExpiresAcrossRestarts(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{MaxDepth: 2, DefaultTTL: time.Hour, MaxTTL: 3 * time.Hour})
	f.requireError(f.deploy(queueBody(map[string]any{"when_busy": "reject", "queue_ttl_seconds": 60})), http.StatusBadRequest, "HTTP-400")
	running := f.mustDeploy(queueBody(nil))
	f.requireError(f.deploy(queueBody(map[string]any{"when_busy": "queue", "queue_ttl_seconds": 3*3600 + 1})), http.StatusBadRequest, "HTTP-400")
	first := f.mustQueue(f.deploy(queueBody(map[string]any{"queue_ttl_seconds": 3 * 3600})))
	require.WithinDuration(t, time.Now().Add(3*time.Hour), first.ExpiresAt, time.Minute)
	second := f.mustQueue(f.deploy(queueBody(map[string]any{"when_busy": "queue", "runner_log_level": "debug"})))
	require.WithinDuration(t, time.Now().Add(time.Hour), second.ExpiresAt, time.Minute)
	f.requireError(f.deploy(queueBody(map[string]any{"when_busy": "queue", "runner_log_level": "warn"})), http.StatusConflict, model.CodeDeploymentQueueFull)

	// Expiry is durable state: a freshly started process expires what an old one accepted.
	f.exec(`UPDATE environment_deployment_queue SET created_at=now()-interval '2 hours', expires_at=now()-interval '1 second' WHERE id=$1`, first.Id)
	restarted := f.newServer(f.openStore())
	require.NoError(t, restarted.SweepDeploymentQueue(f.ctx))
	restarted.WaitDeploymentQueueKicks()
	expired := f.item(first.Id)
	require.Equal(t, model.DeploymentQueueExpired, expired.State)
	require.Equal(t, opt.Of(model.QueueReasonExpired), expired.ReasonCode)
	require.Equal(t, 1, f.item(second.Id).Position)

	// A head that expires while waiting is settled on promotion instead of dispatched.
	f.exec(`UPDATE environment_deployment_queue SET created_at=now()-interval '2 hours', expires_at=now()-interval '1 second' WHERE id=$1`, second.Id)
	f.finish(running.Id, model.DeploymentStatusSucceeded)
	require.Equal(t, model.DeploymentQueueExpired, f.item(second.Id).State)
	require.Len(t, f.deployments(), 1)
	require.Empty(t, f.core.unsettledGuards())
}

func TestDeploymentQueueCancellationRacingPromotionSettlesExactlyOnce(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	cancelPath := func(id uuid.UUID) string {
		return "/orgs/" + queueOrg + "/deployment-queue/" + id.String() + "/actions/cancel"
	}
	running := f.mustDeploy(queueBody(nil))

	t.Run("cancel during promotion loses", func(t *testing.T) {
		item := f.mustQueue(f.deploy(queueBody(queueEverything)))
		entered, release := make(chan struct{}), make(chan struct{})
		f.core.hookAuthorize(t, func(uuid.UUID) {
			close(entered)
			<-release
		})
		f.complete(running.Id, model.DeploymentStatusSucceeded)
		require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, running.Id))
		done := make(chan error, 1)
		go func() { done <- f.server.PromoteDeploymentQueue(f.ctx, f.core.envUUID) }()
		<-entered
		f.requireError(f.call(http.MethodPost, cancelPath(item.Id), map[string]any{"reason": "obsolete"}, f.actor, ""), http.StatusConflict, model.CodeQueueItemDispatching)
		close(release)
		require.NoError(t, <-done)
		settled := f.item(item.Id)
		require.Equal(t, model.DeploymentQueueDispatched, settled.State)
		require.False(t, settled.SettledBy.IsSet())
		f.requireError(f.call(http.MethodPost, cancelPath(item.Id), map[string]any{"reason": "obsolete"}, f.actor, ""), http.StatusConflict, model.CodeQueueItemSettled)
		running = Deployment{Id: settled.DeploymentID.Must()}
	})

	t.Run("cancel before promotion wins", func(t *testing.T) {
		item := f.mustQueue(f.deploy(queueBody(queueEverything)))
		other := uuid.New()
		f.iam.deny(other, true)
		require.Equal(t, http.StatusForbidden, f.call(http.MethodPost, cancelPath(item.Id), nil, other, "").Code, "cancelling needs deployment_write")
		response := f.call(http.MethodPost, cancelPath(item.Id), map[string]any{"reason": "superseded by hotfix"}, f.actor, "")
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var cancelled DeploymentQueueItem
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &cancelled))
		require.Equal(t, Cancelled, cancelled.State)
		require.Equal(t, f.actor, *cancelled.SettledBy)
		require.Equal(t, "superseded by hotfix", *cancelled.ReasonMessage)
		require.Equal(t, http.StatusOK, f.call(http.MethodPost, cancelPath(item.Id), map[string]any{"reason": "superseded by hotfix"}, f.actor, "").Code, "exact replay")
		f.requireError(f.call(http.MethodPost, cancelPath(item.Id), map[string]any{"reason": "changed my mind"}, f.actor, ""), http.StatusConflict, model.CodeQueueItemSettled)
		before := len(f.deployments())
		f.finish(running.Id, model.DeploymentStatusSucceeded)
		require.Len(t, f.deployments(), before, "a cancelled item never dispatches")
		running = f.mustDeploy(queueBody(nil))
	})

	t.Run("concurrent cancel and promotion", func(t *testing.T) {
		for round := range 12 {
			item := f.mustQueue(f.deploy(queueBody(queueEverything)))
			f.complete(running.Id, model.DeploymentStatusSucceeded)
			require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, running.Id))
			before := len(f.deployments())
			var wg sync.WaitGroup
			var cancelCode int
			wg.Add(2)
			go func() {
				defer wg.Done()
				cancelCode = f.call(http.MethodPost, cancelPath(item.Id), nil, f.actor, "").Code
			}()
			go func() {
				defer wg.Done()
				assert.NoError(t, f.server.PromoteDeploymentQueue(f.ctx, f.core.envUUID))
			}()
			wg.Wait()
			f.server.WaitDeploymentQueueKicks()
			settled := f.item(item.Id)
			switch settled.State {
			case model.DeploymentQueueDispatched:
				require.Equal(t, http.StatusConflict, cancelCode, "round %d", round)
				require.Len(t, f.deployments(), before+1)
				running = Deployment{Id: settled.DeploymentID.Must()}
			case model.DeploymentQueueCancelled:
				require.Equal(t, http.StatusOK, cancelCode, "round %d", round)
				require.False(t, settled.DeploymentID.IsSet())
				require.Len(t, f.deployments(), before)
				running = f.mustDeploy(queueBody(nil))
			default:
				t.Fatalf("round %d left the item %s", round, settled.State)
			}
		}
	})
}

// No leader: two processes and many promoters race; run it under -race.
func TestDeploymentQueueManyPromotersInTwoProcessesDispatchEachItemOnce(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{MaxDepth: 10})
	second := f.newServer(f.openStore())
	processes := []*Server{f.server, second}
	running := f.mustDeploy(queueBody(nil))
	items := make([]uuid.UUID, 8)
	for index := range items {
		items[index] = f.mustQueue(f.deploy(queueBody(map[string]any{"when_busy": "queue", "runner_log_level": []string{"debug", "info", "warn", "error"}[index%4]}))).Id
	}
	current := running.Id
	for round := 0; round < 3*len(items) && len(f.queued()) > 0; round++ {
		f.complete(current, model.DeploymentStatusSucceeded)
		require.NoError(t, processes[round%2].DeploymentAdmissions.SettleDeployment(f.ctx, current))
		var wg sync.WaitGroup
		for promoter := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				process := processes[promoter%2]
				if promoter%4 == 0 {
					process.KickDeploymentQueue(f.core.envUUID)
					return
				}
				assert.NoError(t, process.PromoteDeploymentQueue(f.ctx, f.core.envUUID))
			}()
		}
		wg.Wait()
		for _, process := range processes {
			process.WaitDeploymentQueueKicks()
		}
		require.LessOrEqual(t, f.count(`SELECT count(*) FROM deployments WHERE status='executing'`), 1, "one Deployment at a time")
		require.LessOrEqual(t, f.count(`SELECT count(*) FROM environment_deployment_leases`), 1, "one lease holder")
		executing := f.count(`SELECT count(*) FROM deployments WHERE status='executing'`)
		require.Equal(t, 1, executing, "round %d promoted nothing", round)
		deployments := f.deployments()
		current = deployments[len(deployments)-1].Id
	}
	require.Empty(t, f.queued())
	deployments := f.deployments()
	require.Len(t, deployments, len(items)+1, "exactly one Deployment per item")
	seen := map[uuid.UUID]bool{}
	for index, id := range items {
		item := f.item(id)
		require.Equal(t, model.DeploymentQueueDispatched, item.State)
		require.False(t, seen[item.DeploymentID.Must()])
		seen[item.DeploymentID.Must()] = true
		require.Equal(t, deployments[index+1].Id, item.DeploymentID.Must(), "FIFO across processes")
	}
	for index := 1; index < len(deployments); index++ {
		previous, err := f.completedAt(deployments[index-1].Id)
		require.NoError(t, err)
		require.False(t, deployments[index].CreatedAt.Before(previous), "Deployments never overlap")
	}
}

func (f *queueFixture) completedAt(deploymentID uuid.UUID) (time.Time, error) {
	deployment, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, deploymentID, model.GetModeDefault)
	if err != nil {
		return time.Time{}, err
	}
	if !deployment.CompletedAt.IsSet() {
		return time.Time{}, fmt.Errorf("deployment %s has not completed", deploymentID)
	}
	return deployment.CompletedAt.Must(), nil
}

func TestDeploymentQueueSurvivesProcessLossAfterCoreAdmission(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	running := f.mustDeploy(queueBody(nil))
	item := f.mustQueue(f.deploy(queueBody(queueEverything)))
	admitted := make(chan uuid.UUID, 1)
	f.core.hookAuthorize(t, func(admission uuid.UUID) {
		assert.NoError(t, f.terminateQueueHolder())
		admitted <- admission
	})
	f.complete(running.Id, model.DeploymentStatusSucceeded)
	require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, running.Id))
	require.Error(t, f.server.PromoteDeploymentQueue(f.ctx, f.core.envUUID), "the process died between Core admission and commit")
	lost := <-admitted

	waiting := f.item(item.Id)
	require.Equal(t, model.DeploymentQueueQueued, waiting.State)
	require.Zero(t, waiting.Attempts)
	require.Len(t, f.deployments(), 1)
	require.Equal(t, []uuid.UUID{lost}, f.core.unsettledGuards())

	// Another attempt meets Core's unresolved guard and waits; nothing is duplicated.
	f.promote(f.server)
	waiting = f.item(item.Id)
	require.Equal(t, model.DeploymentQueueQueued, waiting.State)
	require.Equal(t, 1, waiting.Attempts)
	require.Contains(t, waiting.LastError.Must(), "settling an earlier Deployment")

	backdateAdmissionReceipt(t, f.ctx, f.store, queueOrg, lost)
	require.NoError(t, f.server.DeploymentAdmissions.ReconcileUnsettled(f.ctx, 10))
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	receipt, err := model.GetDeploymentAdmissionReceipt(f.ctx, tx, queueOrg, lost)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Equal(t, "absent", receipt.Disposition, "the lost admission is sealed absent")
	f.promote(f.server)
	require.Equal(t, model.DeploymentQueueDispatched, f.item(item.Id).State)
	require.Len(t, f.deployments(), 2)
}

func TestDeploymentQueuePromotesWhenCoreSettlesWithoutWaitingForTheSweeper(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	running := f.mustDeploy(queueBody(nil))
	f.complete(running.Id, model.DeploymentStatusSucceeded)
	// The Data Plane is idle, Core is not: the request waits instead of failing.
	item := f.mustQueue(f.deploy(queueBody(queueEverything)))
	f.promote(f.server)
	waiting := f.item(item.Id)
	require.Equal(t, model.DeploymentQueueQueued, waiting.State)
	require.GreaterOrEqual(t, waiting.Attempts, 1)
	require.True(t, waiting.NextAttemptAt.After(time.Now()), "transient failures back off")
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(t, err)
	due, err := model.DueDeploymentQueueEnvironments(f.ctx, tx, 10)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	require.Empty(t, due, "the sweeper would not retry yet")

	f.settle(f.server, running.Id)
	dispatched := f.item(item.Id)
	require.Equal(t, model.DeploymentQueueDispatched, dispatched.State)
	require.Len(t, f.deployments(), 2)
}

func TestDeploymentQueuePermanentRejectionsSkipOnlyTheirItem(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	pinned, forbidden := uuid.New(), uuid.New()
	f.core.set(func(c *queueCore) { c.deny[pinned] = "pin_protection" })
	running := f.mustDeploy(queueBody(nil))
	path := "/orgs/" + queueOrg + "/deployments"
	pinItem := f.mustQueue(f.call(http.MethodPost, path, queueBody(queueEverything), pinned, ""))
	prunedItem := f.mustQueue(f.deploy(queueBody(map[string]any{"when_busy": "queue", "manifest": nil, "mode": "rollback", "rollback_to_deployment_id": uuid.New()})))
	forbiddenItem := f.mustQueue(f.call(http.MethodPost, path, queueBody(map[string]any{"when_busy": "queue", "runner_log_level": "debug"}), forbidden, ""))
	baseItem := f.mustQueue(f.deploy(queueBody(map[string]any{"when_busy": "queue", "expected_base_deployment_id": uuid.New()})))
	validItem := f.mustQueue(f.deploy(queueBody(map[string]any{"when_busy": "queue", "expected_base_deployment_id": running.Id})))
	f.iam.deny(forbidden, true)

	f.finish(running.Id, model.DeploymentStatusSucceeded)
	// Only the changed base refers to a Deployment: the Environment's current one, which replaced the expected base.
	reasonDeployments := map[uuid.UUID]opt.Opt[uuid.UUID]{baseItem.Id: opt.Of(running.Id)}
	for id, reason := range map[uuid.UUID]string{pinItem.Id: "pin_protection", prunedItem.Id: deploymentQueueReasonNotFound,
		forbiddenItem.Id: deploymentQueueReasonForbidden, baseItem.Id: model.CodeDeploymentBaseChanged} {
		rejected := f.item(id)
		require.Equal(t, model.DeploymentQueueRejected, rejected.State, reason)
		require.Equal(t, opt.Of(reason), rejected.ReasonCode)
		require.NotEmpty(t, rejected.ReasonMessage.Must())
		require.Equal(t, reasonDeployments[id], rejected.ReasonDeploymentID, reason)
		require.False(t, rejected.DeploymentID.IsSet())
	}
	valid := f.item(validItem.Id)
	require.Equal(t, model.DeploymentQueueDispatched, valid.State)
	require.Len(t, f.deployments(), 2)
	require.Len(t, f.core.unsettledGuards(), 1, "every rejected attempt sealed its Core admission")
	detail := f.apiItem(baseItem.Id)
	require.Equal(t, Rejected, detail.State)
	require.Equal(t, model.CodeDeploymentBaseChanged, *detail.ReasonCode)
	require.Equal(t, running.Id, *detail.ReasonDeploymentId)
	require.NotNil(t, detail.Manifest, "a single item shows its requested manifest")
}

func TestDeploymentQueueUnreconciledPluginFailureRejectsWaitingWorkAndAcknowledgementStartsNothing(t *testing.T) {
	for _, path := range []string{"promotion", "acknowledgement"} {
		t.Run(path, func(t *testing.T) {
			f := newQueueFixture(t, DeploymentQueueConfig{})
			plugin := f.mustDeploy(queueBody(nil))
			f.markPluginDispatched(plugin.Id)
			first := f.mustQueue(f.deploy(queueBody(queueEverything)))
			second := f.mustQueue(f.deploy(queueBody(map[string]any{"when_busy": "queue", "runner_log_level": "debug"})))
			_, err := f.store.UpdateDeploymentStatusAndOutputs(f.ctx, nil, plugin.Id, model.UpdateDeploymentStatusAndOutputsParams{Status: model.DeploymentStatusFailed, StatusMessage: "watchdog timeout"})
			require.NoError(t, err)
			require.NoError(t, f.server.DeploymentAdmissions.SettleDeployment(f.ctx, plugin.Id))
			if path == "promotion" {
				f.promote(f.server)
			}
			f.requireError(f.deploy(queueBody(queueEverything)), http.StatusConflict, model.CodeOutcomeUnreconciled)
			f.requireError(f.deploy(queueBody(nil)), http.StatusConflict, model.CodeOutcomeUnreconciled)

			f.exec(`UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours', deadline=now()-interval '1 hour' WHERE deployment_id=$1`, plugin.Id)
			acknowledged := f.call(http.MethodPost, "/internal/orgs/"+queueOrg+"/deployments/"+plugin.Id.String()+"/actions/acknowledge-outcome", map[string]any{"reason": "runner job removed"}, f.actor, "")
			require.Equal(t, http.StatusCreated, acknowledged.Code, acknowledged.Body.String())
			rejectedBy := map[string]string{"promotion": "may still have an active Runner", "acknowledgement": "failed with an unreconciled outcome"}[path]
			for _, id := range []uuid.UUID{first.Id, second.Id} {
				rejected := f.item(id)
				require.Equal(t, model.DeploymentQueueRejected, rejected.State)
				require.Equal(t, opt.Of(model.CodeOutcomeUnreconciled), rejected.ReasonCode)
				require.Contains(t, rejected.ReasonMessage.Must(), rejectedBy)
				require.Equal(t, opt.Of(plugin.Id), rejected.ReasonDeploymentID, "the reason names the fencing Deployment")
			}
			f.promote(f.server)
			require.NoError(t, f.server.SweepDeploymentQueue(f.ctx))
			require.Len(t, f.deployments(), 1, "acknowledging never starts queued work")

			next := f.mustDeploy(queueBody(queueEverything))
			f.mustQueue(f.deploy(queueBody(queueEverything)))
			require.NotEqual(t, plugin.Id, next.Id)
		})
	}
}

func (f *queueFixture) markPluginDispatched(deploymentID uuid.UUID) {
	f.t.Helper()
	deployment, _, _, _, err := f.store.GetDeployment(f.ctx, nil, queueOrg, deploymentID, model.GetModeDefault)
	require.NoError(f.t, err)
	_, err = model.BeginDeploymentDispatch(f.ctx, f.store, deployment)
	require.NoError(f.t, err)
	tx, err := f.store.BeginTx(f.ctx, nil)
	require.NoError(f.t, err)
	operation := uuid.New()
	_, err = model.StoreExternalOperationMaterial(f.ctx, tx, model.ExternalOperationMaterial{OrgID: queueOrg, PluginID: "dev.example.rollouts", OperationRef: operation, Recipient: "age1recipient", EncryptedIdentity: "vault:v1:wrapped"})
	require.NoError(f.t, err)
	_, err = model.CreateExternalOperationDeploymentReceipt(f.ctx, tx, model.ExternalOperationDeploymentReceipt{
		ExternalOperationDeploymentKey: model.ExternalOperationDeploymentKey{OrgID: queueOrg, PluginID: "dev.example.rollouts", RequestID: "dispatch-" + deploymentID.String()},
		OperationRef:                   operation.String(), Fence: 1, AssertionAction: ExternalDeploymentAction, ServiceAction: ExternalDeploymentAction,
		RequestBodySHA256: fmt.Sprintf("%064d", 0), ActorID: f.actor.String(), ProjectID: queueProject, EnvID: queueEnv,
		ProjectUUID: opt.Of(f.core.projectUUID), EnvironmentUUID: opt.Of(f.core.envUUID), DeploymentID: deploymentID,
	})
	require.NoError(f.t, err)
	require.NoError(f.t, tx.Commit())
}

func TestDeploymentQueueWaitsBehindAPlanRunAndNeverQueuesPlans(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	plan := f.mustDeploy(queueBody(map[string]any{"plan_only": true}))
	// Never queued, whatever the request says; a second plan cannot start beside the first, both take the state lock.
	for _, body := range []map[string]any{
		{"when_busy": "queue", "plan_only": true}, {"when_busy": "queue", "mode": "plan_only"}, {"plan_only": true, "halt_on_failure": true, "queue_ttl_seconds": 60},
	} {
		f.requireError(f.deploy(queueBody(body)), http.StatusConflict, model.CodeEnvironmentBusy)
	}
	for _, body := range []map[string]any{{"is_dry_run": true}, {"when_busy": "queue", "is_dry_run": true}} {
		require.Nil(t, f.dryRunPendingChanges(body), "dry runs answer while a plan executes, which changes nothing")
	}
	require.Zero(t, f.count(`SELECT count(*) FROM environment_deployment_queue`))

	item := f.mustQueue(f.deploy(queueBody(queueEverything)))
	f.complete(plan.Id, model.DeploymentStatusSucceeded)
	f.promote(f.server)
	dispatched := f.item(item.Id)
	require.Equal(t, model.DeploymentQueueDispatched, dispatched.State)
	deployments := f.deployments()
	require.Len(t, deployments, 2)
	require.Equal(t, model.DeploymentModeDeploy, deployments[1].Mode)
}

func TestDeploymentQueueRefusesDeletingEnvironmentsAndOrganizationTokens(t *testing.T) {
	f := newQueueFixture(t, DeploymentQueueConfig{})
	f.mustDeploy(queueBody(nil))
	// An organization token leaves no user whose authority promotion could re-check.
	tokenRequest := f.request(http.MethodPost, "/orgs/"+queueOrg+"/deployments", queueBody(queueEverything), f.actor, "")
	tokenRequest.Header.Set("Po-Org-Id", queueOrg)
	f.requireError(f.serve(tokenRequest), http.StatusConflict, model.CodeEnvironmentBusy)
	require.Empty(t, f.queued())

	f.core.set(func(c *queueCore) { c.status = coreapi.EnvironmentStatusDeleting })
	f.requireError(f.deploy(queueBody(queueEverything)), http.StatusConflict, model.CodeEnvironmentDeleting)
	require.Empty(t, f.queued())
}
