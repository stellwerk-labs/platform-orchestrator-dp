package api

import (
	"testing"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/nativelogs"
	"github.com/stretchr/testify/require"
)

func TestNativeLogSelectionUsesRevisionNotArrival(t *testing.T) {
	capture := func(revision int64) nativelogs.Descriptor {
		return nativelogs.Descriptor{Status: nativelogs.PendingTransfer, Authority: nativelogs.Authority{Phase: "plan", ExecutionID: "same-execution", CaptureRevision: revision}}
	}
	newer, older := capture(2), capture(1)
	p := model.PipelineProgress{LogReferences: []nativelogs.Descriptor{newer, older}, LogCapture: &older}
	selected, status := selectNativeLog(p, "plan", "same-execution", nil)
	require.Equal(t, "available", status)
	require.Equal(t, newer, *selected)
	revision := int64(1)
	selected, status = selectNativeLog(p, "plan", "same-execution", &revision)
	require.Equal(t, "available", status)
	require.Equal(t, older, *selected)
	selected, status = selectNativeLog(p, "apply", "", nil)
	require.Nil(t, selected)
	require.Equal(t, "not_captured", status)
	p.LogReferencesLimited = true
	selected, status = selectNativeLog(p, "plan", "unprojected-execution", nil)
	require.Nil(t, selected)
	require.Equal(t, "reference_limit", status)
	p.LogReferences = nil
	p.LogReferencesLimited = false
	selected, status = selectNativeLog(p, "plan", "same-execution", nil)
	require.Nil(t, selected)
	require.Equal(t, nativelogs.PendingTransfer, status)
}
