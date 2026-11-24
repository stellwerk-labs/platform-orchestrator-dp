package model

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPipelineExecutionLimitsRefuseAmbiguousOrUnboundedAuthority(t *testing.T) {
	for _, raw := range []string{
		`null`, `{}`, `{"plan_seconds":1,"apply_seconds":1}`,
		`{"plan_seconds":null,"apply_seconds":1,"task_seconds":1}`,
		`{"plan_seconds":0,"apply_seconds":1,"task_seconds":1}`,
		`{"plan_seconds":-1,"apply_seconds":1,"task_seconds":1}`,
		`{"plan_seconds":14401,"apply_seconds":1,"task_seconds":1}`,
		`{"plan_seconds":1,"apply_seconds":86401,"task_seconds":1}`,
		`{"plan_seconds":1,"apply_seconds":1,"task_seconds":14401}`,
		`{"plan_seconds":9223372036854775808,"apply_seconds":1,"task_seconds":1}`,
		`{"plan_seconds":1.5,"apply_seconds":1,"task_seconds":1}`,
		`{"plan_seconds":1,"apply_seconds":1,"task_seconds":1,"other":1}`,
		`{"PLAN_SECONDS":1,"apply_seconds":1,"task_seconds":1}`,
		`{"plan_seconds apply_seconds":1,"apply_seconds":1,"task_seconds":1}`,
		`{"plan_seconds":1,"plan_seconds":2,"apply_seconds":1,"task_seconds":1}`,
	} {
		t.Run(raw, func(t *testing.T) {
			var limits PipelineExecutionLimits
			require.Error(t, json.Unmarshal([]byte(raw), &limits))
		})
	}
	for _, raw := range []string{
		`{"task_seconds":1,"plan_seconds":1,"apply_seconds":1}`,
		`{"plan_seconds":14400,"apply_seconds":86400,"task_seconds":14400}`,
	} {
		var limits PipelineExecutionLimits
		require.NoError(t, json.Unmarshal([]byte(raw), &limits))
		require.True(t, limits.Valid())
	}
}
