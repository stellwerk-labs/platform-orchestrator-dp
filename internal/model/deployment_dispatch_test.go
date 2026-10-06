package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnlyPluginDispatchedUnknownFailureBlocksReplacement(t *testing.T) {
	unknownPluginFailure := DeploymentStopEvidence{Failed: true, PluginDispatched: true, Dispatched: true}
	for name, test := range map[string]struct {
		evidence     DeploymentStopEvidence
		unreconciled bool
		blocks       bool
	}{
		"plugin-dispatched failure without stop proof": {evidence: unknownPluginFailure, unreconciled: true, blocks: true},
		"ordinary failure without stop proof":          {evidence: DeploymentStopEvidence{Failed: true, Dispatched: true}, unreconciled: true},
		"plugin-dispatched failure with stop proof":    {evidence: DeploymentStopEvidence{Failed: true, PluginDispatched: true, Dispatched: true, StopProven: true}},
		"plugin-dispatched failure never dispatched":   {evidence: DeploymentStopEvidence{Failed: true, PluginDispatched: true}},
		"plugin-dispatched success":                    {evidence: DeploymentStopEvidence{PluginDispatched: true, Dispatched: true}, unreconciled: true},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.unreconciled, test.evidence.OutcomeUnreconciled())
			require.Equal(t, test.blocks, test.evidence.BlocksReplacement())
		})
	}
}
