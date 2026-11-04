package model

import (
	"testing"

	observation "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/observationapi"
	"github.com/stretchr/testify/require"
)

func TestObservationResultOnlyAcceptsKnownReplacementActionPairs(t *testing.T) {
	for _, test := range []struct {
		actions []observation.ObservationChangeActions
		valid   bool
	}{
		{actions: []observation.ObservationChangeActions{"create"}, valid: true},
		{actions: []observation.ObservationChangeActions{"read"}, valid: true},
		{actions: []observation.ObservationChangeActions{"update"}, valid: true},
		{actions: []observation.ObservationChangeActions{"delete"}, valid: true},
		{actions: []observation.ObservationChangeActions{"no-op"}, valid: true},
		{actions: []observation.ObservationChangeActions{"forget"}, valid: true},
		{actions: []observation.ObservationChangeActions{"create", "delete"}, valid: true},
		{actions: []observation.ObservationChangeActions{"delete", "create"}, valid: true},
		{actions: []observation.ObservationChangeActions{"delete", "delete"}},
		{actions: []observation.ObservationChangeActions{"update", "delete"}},
		{actions: []observation.ObservationChangeActions{"read", "create"}},
	} {
		result := observation.ObservationResult{SchemaVersion: "1", Complete: true, Tool: "terraform", ToolVersion: "1.9.0", ObservedChanges: []observation.ObservationChange{}, ProposedChanges: []observation.ObservationChange{{Id: "r1", ResourceType: "test_resource", Actions: test.actions}}}
		err := ValidateObservationResult(result)
		if test.valid {
			require.NoError(t, err, "actions %v", test.actions)
		} else {
			require.Error(t, err, "actions %v", test.actions)
		}
	}
}
