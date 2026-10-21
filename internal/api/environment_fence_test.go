package api

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestDeploymentSummaryReportsTheEnvironmentFenceOnlyWhileFenced(t *testing.T) {
	spec, err := GetSwagger()
	require.NoError(t, err)
	schema := spec.Components.Schemas["DeploymentSummary"].Value
	id := uuid.New()
	failedAt := time.Date(2026, 10, 7, 8, 0, 0, 123456000, time.UTC)
	summary := func(fence opt.Opt[model.EnvironmentFence]) map[string]any {
		t.Helper()
		encoded, err := json.Marshal(apiDepSummaryFromModel(model.DeploymentSummary{
			OrgId: "org", ProjectId: "project", EnvId: "env", Id: id, CreatedAt: failedAt.Add(-time.Minute), CreatedBy: uuid.New(),
			CompletedAt: opt.Of(failedAt), Mode: model.DeploymentModeDeploy, Status: model.DeploymentStatusFailed, StatusMessage: "watchdog",
			EnvironmentFence: fence,
		}))
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		require.NoError(t, schema.VisitJSON(decoded), "the summary conforms to the published schema")
		return decoded
	}

	fenced := summary(opt.Of(model.EnvironmentFence{DeploymentID: id, Since: failedAt, RemedyAvailableAt: opt.Of(failedAt.Add(time.Hour))}))
	require.Equal(t, map[string]any{
		"code":                "outcome_unreconciled",
		"deployment_id":       id.String(),
		"since":               "2026-10-07T08:00:00.123456Z",
		"remedy":              "acknowledge-outcome",
		"remedy_available_at": "2026-10-07T09:00:00.123456Z",
	}, fenced["environment_fence"])

	withoutWindow := summary(opt.Of(model.EnvironmentFence{DeploymentID: id, Since: failedAt}))
	require.Equal(t, map[string]any{
		"code":          "outcome_unreconciled",
		"deployment_id": id.String(),
		"since":         "2026-10-07T08:00:00.123456Z",
		"remedy":        "acknowledge-outcome",
	}, withoutWindow["environment_fence"], "no recorded window means the remedy is available at once")

	require.NotContains(t, summary(opt.Empty[model.EnvironmentFence]()), "environment_fence", "an unfenced Environment carries no fence at all")
}
