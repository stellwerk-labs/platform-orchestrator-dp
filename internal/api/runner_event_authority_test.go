package api

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stretchr/testify/require"
)

func TestStoredNativeEventAuthorityRejectsEveryMalformedTargetField(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*cp.NativeExecutionTargetReference)
	}{
		{"domain", func(v *cp.NativeExecutionTargetReference) { v.RunnerId = "other" }},
		{"uuid", func(v *cp.NativeExecutionTargetReference) { v.RunnerUuid = uuid.Nil }},
		{"runtime", func(v *cp.NativeExecutionTargetReference) { v.RuntimeVersion = "unknown" }},
		{"pipeline", func(v *cp.NativeExecutionTargetReference) { v.PipelineVersion = "unknown" }},
		{"tools", func(v *cp.NativeExecutionTargetReference) { v.ToolsDigest = "invalid" }},
		{"backend", func(v *cp.NativeExecutionTargetReference) { v.StateBackendType = "postgres" }},
		{"configuration", func(v *cp.NativeExecutionTargetReference) { v.StateConfigurationDigest = "invalid" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			target := nativeTargetFixture()
			dep := &model.DeploymentSummary{Id: uuid.New(), OrgId: "org", RunnerId: target.RunnerId, NativeTarget: target}
			token, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
			require.NoError(t, err)
			envelope := hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, OrganizationID: dep.OrgId, RunnerID: token, DeploymentID: dep.Id.String(), EventID: dep.Id.String() + ":deployment-result", Type: "deployment-result"}
			event := &RunnerEventMetadata{EventID: envelope.EventID, RunnerID: token, Type: envelope.Type, Envelope: &envelope}
			id, err := runnerEventInboxID(dep, event)
			require.NoError(t, err)
			require.NotEqual(t, envelope.EventID, id)
			test.change(target)
			_, err = runnerEventInboxID(dep, event)
			require.ErrorIs(t, err, ErrRunnerEventTargetInvalid)
		})
	}
}
