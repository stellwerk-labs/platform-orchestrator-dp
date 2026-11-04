package model

import (
	"encoding/json"
	"testing"

	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/require"
)

func TestRunnerAdmissionConfigurationIsOptionalAndExplicit(t *testing.T) {
	for _, test := range []struct {
		raw       string
		limit     int
		supported bool
		invalid   bool
	}{
		{raw: `{"type":"kubernetes"}`, limit: 0},
		{raw: `{"type":"kubernetes-agent","max_parallel_jobs":2,"supported_protocols":["environment-observation/v1"]}`, limit: 2, supported: true},
		{raw: `{"type":"serverless-ecs","max_parallel_jobs":1}`, limit: 1},
		{raw: `{"type":"kubernetes","max_parallel_jobs":0}`, invalid: true},
		{raw: `{"type":"kubernetes","max_parallel_jobs":-1}`, invalid: true},
	} {
		t.Run(test.raw, func(t *testing.T) {
			var config cp.RunnerConfiguration
			require.NoError(t, json.Unmarshal([]byte(test.raw), &config))
			limit, supported, err := RunnerAdmissionConfiguration(config)
			if test.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.limit, limit)
				require.Equal(t, test.supported, supported)
			}
		})
	}
}
