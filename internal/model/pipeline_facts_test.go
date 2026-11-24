package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineFactsDropPrivatePayloadAndKeepPublicStepEvidence(t *testing.T) {
	raw := []byte(
		`{"id":"id","environment_id":"env","phase":"completed","revision":2,"plan_changes":{"add":1,"change":2,"remove":3},"apply_changes":{"add":0,"change":2,"remove":0},"steps":[{"id":"check","phase":"plan_check","status":"completed","outcome":{"verdict":"allow","meaning":"Exact plan satisfies the resource check","tool_id":"resource-policy","private_stdout":"CANARY_SECRET"}}],"plan_capture":{"secret":"CANARY_SECRET"},"logs":"CANARY_SECRET"}`,
	)
	var progress PipelineProgress
	require.NoError(t, json.Unmarshal(raw, &progress))
	public, err := json.Marshal(progress)
	require.NoError(t, err)
	require.NotContains(t, string(public), "CANARY_SECRET")
	require.Equal(t, 1, progress.PlanChanges.Add)
	require.Zero(t, progress.ApplyChanges.Add)
	require.Equal(t, "allow", progress.Steps[0].Outcome.Verdict)
}
