//go:build integration

package model

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestLastDeploymentsReportTheFenceOfExactlyTheFencedEnvironment(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	db := store.(*databaser)
	params := func() CreateDeploymentParams {
		return CreateDeploymentParams{DeploymentEnvUuid: uuid.New(), Mode: DeploymentModeDeploy, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info",
			Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}")}
	}
	ordinary := dispatchedFailure(t, ctx, store, "a", params(), false)
	fencedParams := params()
	fenced := dispatchedFailure(t, ctx, store, "e", fencedParams, true)
	proven := dispatchedFailure(t, ctx, store, "z", params(), true)
	require.NoError(t, RecordDeploymentCompletion(ctx, db, proven.Id))
	var since, window time.Time
	require.NoError(t, db.QueryRowContext(ctx, `SELECT d.completed_at, b.deadline FROM deployments d
		JOIN deployment_dispatch_budgets b ON b.deployment_id=d.id WHERE d.id=$1`, fenced.Id).Scan(&since, &window))

	// list pages through the organization, optionally inside a transaction: the
	// fence reads must not run while the page's row set still holds the connection.
	list := func(t *testing.T, inTx bool, stateChangeOnly bool, perPage int) []DeploymentSummary {
		t.Helper()
		var tx Tx
		if inTx {
			open, err := store.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { require.NoError(t, open.Rollback()) }()
			tx = open
		}
		var all []DeploymentSummary
		pages, token := 0, ""
		for {
			page, next, err := store.ListLastDeployments(ctx, tx, "unknown-org", token, perPage, ListLastDeploymentsParams{StateChangeOnly: stateChangeOnly})
			require.NoError(t, err)
			require.LessOrEqual(t, len(page), perPage)
			all = append(all, page...)
			pages++
			if next == "" {
				require.Equal(t, (3+perPage-1)/perPage, pages, "paging is unchanged")
				return all
			}
			token = next
		}
	}

	for _, inTx := range []bool{false, true} {
		for _, stateChangeOnly := range []bool{false, true} {
			for _, perPage := range []int{1, 2, 3, 10} {
				t.Run(fmt.Sprintf("tx=%t/state_change_only=%t/per_page=%d", inTx, stateChangeOnly, perPage), func(t *testing.T) {
					items := list(t, inTx, stateChangeOnly, perPage)
					require.Len(t, items, 3)
					require.Equal(t, []uuid.UUID{ordinary.Id, fenced.Id, proven.Id}, []uuid.UUID{items[0].Id, items[1].Id, items[2].Id})
					require.False(t, items[0].EnvironmentFence.IsSet(), "an ordinary Core failure never fences")
					require.False(t, items[2].EnvironmentFence.IsSet(), "Runner stop proof lifts the fence")
					require.True(t, items[1].EnvironmentFence.IsSet())
					fence := items[1].EnvironmentFence.Must()
					require.Equal(t, fenced.Id, fence.DeploymentID)
					require.True(t, since.Equal(fence.Since), "since is when the Deployment failed: %s vs %s", since, fence.Since)
					require.True(t, fence.RemedyAvailableAt.IsSet())
					require.True(t, window.Equal(fence.RemedyAvailableAt.Must()), "the remedy waits for the dispatch window: %s vs %s", window, fence.RemedyAvailableAt.Must())
				})
			}
		}
	}
	require.ErrorIs(t, replaceHead(ctx, store, fencedParams), ErrOutcomeUnreconciled, "the signal and the refusal agree")

	_, err := db.ExecContext(ctx, `UPDATE deployment_dispatch_budgets SET first_attempt_at=now()-interval '2 hours', deadline=now()-interval '1 hour' WHERE deployment_id=$1`, fenced.Id)
	require.NoError(t, err)
	require.True(t, list(t, false, true, 10)[1].EnvironmentFence.IsSet(), "a closed dispatch window alone lifts nothing")
	_, _, err = acknowledge(ctx, store, fenced.Id, uuid.New(), "runner job deleted from the cluster")
	require.NoError(t, err)
	for _, stateChangeOnly := range []bool{false, true} {
		for _, item := range list(t, false, stateChangeOnly, 10) {
			require.False(t, item.EnvironmentFence.IsSet(), "%s/%s: the acknowledgement lifts the fence", item.ProjectId, item.EnvId)
		}
	}
	require.NoError(t, replaceHead(ctx, store, fencedParams), "the Environment accepts Deployments again exactly when the signal is gone")
	for _, stateChangeOnly := range []bool{false, true} {
		replacement := list(t, false, stateChangeOnly, 10)[1]
		require.NotEqual(t, fenced.Id, replacement.Id)
		require.False(t, replacement.EnvironmentFence.IsSet())
	}
}
