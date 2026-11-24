package api

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNativeRunnerCannotSilentlyIgnoreInjectedJobIdentity(t *testing.T) {
	for _, field := range []string{"environment", "secrets", "pod_template"} {
		t.Run(field, func(t *testing.T) {
			cfg := map[string]any{"job": map[string]any{field: map[string]string{"credentials": "configured"}}}
			require.ErrorContains(t, validateNativeJobConfiguration(cfg), "credential injection")
		})
	}
	require.NoError(t, validateNativeJobConfiguration(map[string]any{"job": map[string]any{"namespace": "registration-shape", "service_account": "metadata"}}))
}
