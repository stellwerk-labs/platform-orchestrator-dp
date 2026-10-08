package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
)

func TestCodedConflictsMatchTheirSentinelThroughWrapping(t *testing.T) {
	busy := errors.Wrap(NewErrCodedConflict(CodeEnvironmentBusy, "environment has an active deployment lease"), "failed to create deployment")
	require.ErrorIs(t, busy, ErrEnvironmentBusy)
	require.NotErrorIs(t, busy, ErrOutcomeUnreconciled)
	conflict, ok := IsErrConflict(busy)
	require.True(t, ok, "coded conflicts remain conflicts for every existing caller")
	require.Equal(t, "environment has an active deployment lease", conflict.Message)
	require.NotErrorIs(t, NewErrConflict("plain"), ErrEnvironmentBusy)
	require.NotErrorIs(t, NewErrConflict("plain"), ErrConflict{}, "an uncoded conflict is no sentinel")
}

func TestConflictsNameTheDeploymentTheyAreAboutThroughWrapping(t *testing.T) {
	failed := uuid.New()
	fenced := errors.Wrap(NewErrCodedConflict(CodeOutcomeUnreconciled, "fenced").AboutDeployment(failed), "failed to create deployment")
	require.ErrorIs(t, fenced, ErrOutcomeUnreconciled, "naming a Deployment keeps the sentinel match")
	conflict, ok := IsErrConflict(fenced)
	require.True(t, ok)
	require.Equal(t, opt.Of(failed), conflict.DeploymentID)
	require.False(t, NewErrCodedConflict(CodeDeploymentBaseChanged, "no current Deployment").AboutDeployment(uuid.Nil).DeploymentID.IsSet(),
		"uuid.Nil names no Deployment")
	require.False(t, NewErrConflict("plain").DeploymentID.IsSet())
}

func TestQueueIntentDigestIdentifiesTheRequestNotTheRequester(t *testing.T) {
	request := NewDeploymentQueueItem{OrgID: "org", ProjectID: "p", EnvID: "e", EnvironmentUUID: uuid.New(), Mode: DeploymentModeDeploy,
		Intent: json.RawMessage(`{"manifest":{"workloads":{}}}`), CreatedBy: uuid.New()}
	digest := request.IntentSHA256()
	require.Len(t, digest, 64)
	sameRequestOtherActor := request
	sameRequestOtherActor.CreatedBy = uuid.New()
	sameRequestOtherActor.TTL = 42
	require.Equal(t, digest, sameRequestOtherActor.IntentSHA256())
	for _, change := range []func(*NewDeploymentQueueItem){
		func(r *NewDeploymentQueueItem) { r.EnvID = "other" },
		func(r *NewDeploymentQueueItem) { r.Mode = DeploymentModeRollback },
		func(r *NewDeploymentQueueItem) { r.Intent = json.RawMessage(`{"manifest":{"workloads":{"api":{}}}}`) },
		func(r *NewDeploymentQueueItem) { r.ExpectedBaseDeploymentID = opt.Of(uuid.New()) },
		func(r *NewDeploymentQueueItem) { r.HaltOnFailure = true },
	} {
		changed := request
		change(&changed)
		require.NotEqual(t, digest, changed.IntentSHA256())
	}
}

// Items queued before halt_on_failure existed must still match their replays.
func TestQueueIntentDigestOfARequestWithoutHaltIsUnchanged(t *testing.T) {
	expectedBase := uuid.MustParse("01234567-89ab-cdef-0123-456789abcdef")
	request := NewDeploymentQueueItem{ProjectID: "p", EnvID: "e", Mode: DeploymentModeRollback,
		Intent: json.RawMessage(`{"rollback_to_deployment_id":"76543210-89ab-cdef-0123-456789abcdef"}`), ExpectedBaseDeploymentID: opt.Of(expectedBase)}
	legacy := sha256.Sum256([]byte(`{"project_id":"p","env_id":"e","mode":"rollback","intent":{"rollback_to_deployment_id":"76543210-89ab-cdef-0123-456789abcdef"},"expected_base_deployment_id":"01234567-89ab-cdef-0123-456789abcdef"}`))
	require.Equal(t, hex.EncodeToString(legacy[:]), request.IntentSHA256())
}
