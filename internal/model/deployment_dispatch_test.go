package model

import (
	"database/sql"
	"testing"
	"time"

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
		"plugin-dispatched failure acknowledged":       {evidence: DeploymentStopEvidence{Failed: true, PluginDispatched: true, Dispatched: true, Acknowledged: true}},
		"plugin-dispatched failure never dispatched":   {evidence: DeploymentStopEvidence{Failed: true, PluginDispatched: true}},
		"plugin-dispatched success":                    {evidence: DeploymentStopEvidence{PluginDispatched: true, Dispatched: true}, unreconciled: true},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.unreconciled, test.evidence.OutcomeUnreconciled())
			require.Equal(t, test.blocks, test.evidence.BlocksReplacement())
		})
	}
}

func TestOutcomeAcknowledgementAcceptsOnlyTheBlockingHeadAfterItsDispatchWindow(t *testing.T) {
	now := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	closed := sql.NullTime{Time: now.Add(-time.Second), Valid: true}
	blocking := DeploymentStopEvidence{Failed: true, PluginDispatched: true, Dispatched: true}
	require.True(t, blocking.BlocksReplacement())
	require.NoError(t, blocking.acknowledgementConflict(true, true, closed, now))
	require.NoError(t, blocking.acknowledgementConflict(true, true, sql.NullTime{}, now), "a missing budget cannot keep a window open")

	for name, test := range map[string]struct {
		evidence  DeploymentStopEvidence
		completed bool
		head      bool
		deadline  sql.NullTime
	}{
		"ordinary Deployment":  {evidence: DeploymentStopEvidence{Failed: true, Dispatched: true}, completed: true, head: true, deadline: closed},
		"not failed":           {evidence: DeploymentStopEvidence{PluginDispatched: true, Dispatched: true}, completed: true, head: true, deadline: closed},
		"not terminal":         {evidence: blocking, head: true, deadline: closed},
		"replaced head":        {evidence: blocking, completed: true, deadline: closed},
		"stop proven":          {evidence: DeploymentStopEvidence{Failed: true, PluginDispatched: true, Dispatched: true, StopProven: true}, completed: true, head: true, deadline: closed},
		"already acknowledged": {evidence: DeploymentStopEvidence{Failed: true, PluginDispatched: true, Dispatched: true, Acknowledged: true}, completed: true, head: true, deadline: closed},
		"dispatch window open": {evidence: blocking, completed: true, head: true, deadline: sql.NullTime{Time: now.Add(time.Minute), Valid: true}},
		"never dispatched":     {evidence: DeploymentStopEvidence{Failed: true, PluginDispatched: true}, completed: true, head: true, deadline: closed},
		"deadline just ahead":  {evidence: blocking, completed: true, head: true, deadline: sql.NullTime{Time: now.Add(time.Nanosecond), Valid: true}},
	} {
		t.Run(name, func(t *testing.T) {
			_, conflict := IsErrConflict(test.evidence.acknowledgementConflict(test.completed, test.head, test.deadline, now))
			require.True(t, conflict)
		})
	}
}
