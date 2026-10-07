package model

import (
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
	} {
		changed := request
		change(&changed)
		require.NotEqual(t, digest, changed.IntentSHA256())
	}
}
