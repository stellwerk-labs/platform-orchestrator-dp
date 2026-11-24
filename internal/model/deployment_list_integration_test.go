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

func TestDeploymentListSearchFiltersAndStablePagination(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	store := dispatchTestStore(t, ctx)
	stamp := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	create := func(org, name, phase string, mode DeploymentMode) uuid.UUID {
		t.Helper()
		tx, err := store.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		dep, err := store.CreateDeployment(ctx, tx, org, "project", uuid.NewString(), CreateDeploymentParams{
			DeploymentEnvUuid: uuid.New(), Mode: mode, CreatedBy: uuid.New(), RunnerId: "runner", RunnerLogLevel: "info",
			Manifest: EncodedDeploymentManifest(`{}`), Graph: EncodedDeploymentGraph(`{}`), Tofu: RawTofu("terraform {}"),
		})
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `UPDATE deployments SET created_at=$2 WHERE id=$1`, dep.Id, stamp)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `INSERT INTO deployment_pipeline_runs(deployment_id,backend,definition,progress) VALUES($1,'native',jsonb_build_object('name',$2::text),jsonb_build_object('phase',$3::text))`, dep.Id, name, phase)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		return dep.Id
	}
	source := create("search-org", "literal 100%_safe", "completed", DeploymentModeDeployPlan)
	child := create("search-org", "saved apply", "post_failed", DeploymentModeDeploy)
	tx, err := store.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, `UPDATE deployment_pipeline_runs SET source_plan_id=$2, source_plan_subject='subject' WHERE deployment_id=$1`, child, source)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	unused := create("search-org", "review-only", "completed", DeploymentModeRollbackPlan)
	approval := create("search-org", "guarded", "awaiting_approval", DeploymentModeDeploy)
	create("other-org", "review-only", "awaiting_approval", DeploymentModeDeployPlan)
	for i := 0; i < 24; i++ {
		create("search-org", fmt.Sprintf("ordinary-%d", i), "completed", DeploymentModeDeploy)
	}

	for _, test := range []struct {
		filter, query string
		want          []uuid.UUID
	}{
		{"unused_plans", "", []uuid.UUID{unused}},
		{"needs_approval", "", []uuid.UUID{approval}},
		{"needs_attention", "", []uuid.UUID{child}},
		{"plans", "100%_", []uuid.UUID{source}},
		{"all", unused.String(), []uuid.UUID{unused}},
		{"all", "' OR 1=1 --", nil},
	} {
		rows, token, err := store.ListDeployments(ctx, nil, "search-org", "", 2, ListDeploymentsParams{Query: test.query, QuickFilter: test.filter})
		require.NoError(t, err)
		require.Empty(t, token)
		ids := make([]uuid.UUID, 0, len(rows))
		for _, row := range rows {
			ids = append(ids, row.Id)
		}
		require.ElementsMatch(t, test.want, ids, "filter=%s query=%s", test.filter, test.query)
	}
	seen := map[uuid.UUID]bool{}
	token := ""
	for page := 0; ; page++ {
		require.Less(t, page, 20)
		rows, next, err := store.ListDeployments(ctx, nil, "search-org", token, 3, ListDeploymentsParams{})
		require.NoError(t, err)
		for _, row := range rows {
			require.False(t, seen[row.Id], "duplicate row")
			seen[row.Id] = true
		}
		if next == "" {
			break
		}
		token = next
	}
	require.Len(t, seen, 28, "equal timestamps must neither drop nor duplicate rows")
	_, _, err = store.ListDeployments(ctx, nil, "search-org", "bad,cursor", 3, ListDeploymentsParams{})
	require.Error(t, err)
	_, _, err = store.ListDeployments(ctx, nil, "search-org", "", 3, ListDeploymentsParams{QuickFilter: "unknown"})
	require.Error(t, err)
	_, _, err = store.ListDeployments(ctx, nil, "search-org", "", 3, ListDeploymentsParams{Query: "bad\x00query"})
	require.Error(t, err)
	legacy, _, err := store.ListDeployments(ctx, nil, "search-org", fmt.Sprint(stamp.Add(time.Second).UnixNano()), 50, ListDeploymentsParams{})
	require.NoError(t, err)
	require.Len(t, legacy, 28)
}
