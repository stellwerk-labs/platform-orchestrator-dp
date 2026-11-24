package api

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const runnerDeploymentResultEventType = "deployment-result"

var storedNativeRunnerID = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,127}$`)
var storedNativeDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

// The stored target is immutable authority. Current Runner registration cannot
// authorize a completion or change the domain identity used by capacity joins.
func runnerEventInboxID(dep *model.DeploymentSummary, event *RunnerEventMetadata) (string, error) {
	invalid := func() (string, error) {
		return "", fmt.Errorf("%w: %w", ErrRunnerEventTargetInvalid, model.NewErrConflict("runner event authority mismatch"))
	}
	if dep.NativeTarget == nil {
		if event == nil {
			return "", nil
		}
		if event.RunnerID != dep.RunnerId || strings.HasPrefix(event.RunnerID, hmessaging.NativeRunnerTokenPrefix) || strings.HasPrefix(event.EventID, hmessaging.NativeMessageIDPrefix) {
			return invalid()
		}
		return event.EventID, nil
	}
	target := dep.NativeTarget
	if event == nil || event.Envelope == nil || target.RunnerId != dep.RunnerId || !storedNativeRunnerID.MatchString(target.RunnerId) || !target.StateBackendType.Valid() || !target.RuntimeVersion.Valid() || !target.PipelineVersion.Valid() || !storedNativeDigest.MatchString(target.ToolsDigest) ||
		!storedNativeDigest.MatchString(target.StateConfigurationDigest) {
		return invalid()
	}
	token, err := hmessaging.NativeRunnerToken(target.RunnerUuid.String())
	if err != nil || token != event.RunnerID {
		return invalid()
	}
	envelope := event.Envelope
	if envelope.ProtocolVersion != hmessaging.ProtocolVersionV1 || envelope.OrganizationID != dep.OrgId || envelope.DeploymentID != dep.Id.String() || envelope.RunnerID != event.RunnerID || envelope.EventID != event.EventID || envelope.Type != event.Type {
		return invalid()
	}
	if event.Type == "rollout-progress" {
		return "", nil
	}
	if event.Type != runnerDeploymentResultEventType || event.EventID != dep.Id.String()+":deployment-result" {
		return invalid()
	}
	id, err := hmessaging.NativeMessageID(dep.OrgId, token, "events", event.Type, event.EventID)
	if err != nil {
		return invalid()
	}
	return id, nil
}
