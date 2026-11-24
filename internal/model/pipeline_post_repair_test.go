package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelinePostRepairMetadataKeepsOnlyExactPublicFields(t *testing.T) {
	good := `{"control_id":"11111111-1111-4111-8111-111111111111","expires_at":"2026-11-21T12:00:00Z"}`
	var value PipelinePostRepairAuthority
	require.NoError(t, json.Unmarshal([]byte(good), &value))
	for _, raw := range []string{
		`null`, `{}`, `{"control_id":"wrong","expires_at":"2026-11-21T12:00:00Z"}`,
		`{"control_id":"11111111-1111-4111-8111-111111111111","expires_at":null}`,
		`{"control_id":"11111111-1111-4111-8111-111111111111","expires_at":"2026-11-21T12:00:00Z","control_id":"11111111-1111-4111-8111-111111111111"}`,
		`{"CONTROL_ID":"11111111-1111-4111-8111-111111111111","expires_at":"2026-11-21T12:00:00Z"}`,
		`{"control_id expires_at":"11111111-1111-4111-8111-111111111111","expires_at":"2026-11-21T12:00:00Z"}`,
		`{"control_id":"11111111-1111-4111-8111-111111111111","expires_at":"2026-11-21T12:00:00Z","outputs":"PRIVATE"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var bad PipelinePostRepairAuthority
			require.Error(t, json.Unmarshal([]byte(raw), &bad))
		})
	}
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	require.JSONEq(t, good, string(encoded))
}
