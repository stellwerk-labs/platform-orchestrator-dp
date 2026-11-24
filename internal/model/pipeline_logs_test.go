package model

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
	"github.com/stretchr/testify/require"
)

func pipelineLogDescriptor(revision int64) nativelogs.Descriptor {
	a := nativelogs.Authority{
		OrganizationID:  "o",
		EnvironmentID:   "e",
		DeploymentID:    "d",
		CommandDigest:   strings.Repeat("a", 64),
		Target:          nativelogs.Target{RunnerID: "r", RunnerUUID: "u", RuntimeVersion: "v2", PipelineVersion: "v2", ToolsDigest: strings.Repeat("b", 64), StateBackendType: "s3", StateConfigurationDigest: strings.Repeat("c", 64)},
		Phase:           "apply",
		ExecutionID:     "d",
		CaptureRevision: revision,
	}
	d := nativelogs.Descriptor{Version: 1, Authority: a, Status: nativelogs.PendingTransfer, CiphertextSHA256: strings.Repeat("d", 64), CiphertextBytes: 100}
	d.ObjectKey = a.ObjectKey(d.CiphertextSHA256)
	return d
}

func TestPipelineLogReferenceImmutableTupleAndFiniteLimits(t *testing.T) {
	first := pipelineLogDescriptor(1)
	second := pipelineLogDescriptor(2)
	refs, limited, err := appendPipelineLogReference(nil, false, second)
	require.NoError(t, err)
	require.False(t, limited)
	refs, limited, err = appendPipelineLogReference(refs, limited, first)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	same, _, err := appendPipelineLogReference(refs, limited, first)
	require.NoError(t, err)
	require.Equal(t, refs, same)
	conflict := first
	conflict.CiphertextSHA256 = strings.Repeat("e", 64)
	conflict.ObjectKey = conflict.Authority.ObjectKey(conflict.CiphertextSHA256)
	_, _, err = appendPipelineLogReference(refs, limited, conflict)
	require.Error(t, err)
	for _, large := range []bool{false, true} {
		t.Run(fmt.Sprint("large=", large), func(t *testing.T) {
			var refs []nativelogs.Descriptor
			limited := false
			for i := int64(1); i <= 129; i++ {
				d := pipelineLogDescriptor(i)
				if large {
					d.Authority.OrganizationID = strings.Repeat("o", 128)
					d.Authority.Target.RuntimeVersion = strings.Repeat("r", 128)
					d.Authority.Target.PipelineVersion = strings.Repeat("p", 128)
					d.Authority.Target.RunnerID = strings.Repeat("r", 128)
					d.Authority.Target.StateBackendType = strings.Repeat("s", 128)
					d.Authority.ExecutionID = strings.Repeat("x", 128)
					d.ObjectKey = d.Authority.ObjectKey(d.CiphertextSHA256)
				}
				before, _ := json.Marshal(refs)
				next, full, err := appendPipelineLogReference(refs, limited, d)
				require.NoError(t, err)
				if full {
					after, _ := json.Marshal(next)
					require.Equal(t, before, after)
					require.True(t, len(refs) < 128 || !large)
					again, sticky, err := appendPipelineLogReference(next, full, d)
					require.NoError(t, err)
					require.True(t, sticky)
					require.Equal(t, next, again)
					limited = true
					break
				}
				refs = next
			}
			require.True(t, limited)
			if !large {
				require.Len(t, refs, 128)
			}
		})
	}
}
