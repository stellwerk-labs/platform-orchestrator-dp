package integrationtests

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker"
	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
)

// More stale events than the consumer's acknowledgement capacity must not
// prevent a real Runner's new result from completing another Deployment.
func TestDeletedRunnerResultsDoNotStarveLiveDeployments(t *testing.T) {
	cp, dp := MustControlPlaneClient(t), MustDataPlaneClient(t)
	orgID := MustCreateOrgId(t, MustInternalControlPlaneClient(t))
	project := MustCreateProject(t, cp, orgID, "result-recovery")
	envType := MustCreateEnvType(t, cp, orgID, "development")
	runner := MustCreateRunnerWithRule(t, cp, orgID, "result-runner", "", "", nil)
	oldEnvironment := MustCreateEnv(t, cp, orgID, envType.Id, project.Id, "old")
	liveEnvironment := MustCreateEnv(t, cp, orgID, envType.Id, project.Id, "live")
	deploy := func(environmentID string) uuid.UUID {
		response, err := dp.CreateDeploymentWithResponse(t.Context(), orgID, &serverclient.CreateDeploymentParams{}, serverclient.DeploymentCreateBody{
			ProjectId: project.Id, EnvId: environmentID, Mode: serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, response.StatusCode(), string(response.Body))
		return response.JSON201.Id
	}
	oldID := deploy(oldEnvironment.Id)
	require.Equal(t, "succeeded", MustWaitForDeploymentComplete(t, dp, orgID, oldID).Status)
	deleted, err := cp.DeleteEnvironmentWithResponse(t.Context(), orgID, project.Id, oldEnvironment.Id, &platformorchestratorcp.DeleteEnvironmentParams{})
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, deleted.StatusCode(), string(deleted.Body))
	require.Eventually(t, func() bool {
		response, err := cp.GetEnvironmentWithResponse(t.Context(), orgID, project.Id, oldEnvironment.Id)
		return err == nil && response.StatusCode() == http.StatusNotFound
	}, 30*time.Second, 100*time.Millisecond, "normal Environment deletion must destroy and remove its Deployment history")
	missing, err := dp.GetDeploymentWithResponse(t.Context(), orgID, oldID)
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, missing.StatusCode())

	connection := MustNATSConn(t)
	js, err := hnats.NewJetStream(connection)
	require.NoError(t, err)
	subscription, err := connection.SubscribeSync("po.dlq.po.v1.orgs." + orgID + ".runners.*.events.*")
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, subscription.Unsubscribe()) })
	require.NoError(t, connection.Flush())
	publisher := hnats.NewPublisher(js, hmessaging.RunnerEventsStreamName, zaptest.NewLogger(t))
	publish := func(deploymentID uuid.UUID, runnerID, eventType string, payload any) string {
		encoded, err := json.Marshal(payload)
		require.NoError(t, err)
		envelope := hmessaging.EventEnvelope{
			ProtocolVersion: hmessaging.ProtocolVersionV1, EventID: uuid.NewString(),
			OrganizationID: orgID, RunnerID: runnerID, DeploymentID: deploymentID.String(),
			Type: eventType, CreatedAt: time.Now().UTC(), Payload: encoded,
		}
		data, err := json.Marshal(envelope)
		require.NoError(t, err)
		subject, err := hmessaging.RunnerEventSubject(orgID, runnerID, eventType)
		require.NoError(t, err)
		require.NoError(t, publisher.Publish(t.Context(), hmessaging.Message{ID: envelope.EventID, Subject: subject, Data: data, CreatedAt: envelope.CreatedAt}))
		return envelope.EventID
	}
	events := map[string]bool{}
	for index := range worker.MainConsumerConcurrency + 2 {
		if index%2 == 0 {
			events[publish(oldID, runner.Id, "deployment-result", map[string]any{"status": "success"})] = true
		} else {
			events[publish(oldID, runner.Id, "runner-error", map[string]any{"code": "COMMAND_EXPIRED", "message": "late terminal event", "retryable": false})] = true
		}
	}
	liveID := deploy(liveEnvironment.Id)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	for range len(events) {
		message, err := subscription.NextMsgWithContext(ctx)
		require.NoError(t, err, "permanently missing targets must enter the retained dead-letter stream promptly")
		var event hmessaging.EventEnvelope
		require.NoError(t, json.Unmarshal(message.Data, &event))
		require.True(t, events[event.EventID], "only this fixture's rejected events are expected")
		delete(events, event.EventID)
		assert.Equal(t, "1", message.Header.Get("Po-Delivery-Attempts"))
	}
	require.Empty(t, events)
	require.Eventually(t, func() bool {
		response, err := dp.GetDeploymentWithResponse(ctx, orgID, liveID)
		return err == nil && response.JSON200 != nil && response.JSON200.Status == "succeeded"
	}, 30*time.Second, 100*time.Millisecond, "a real Runner result must not be starved by stale result retries")

	// Conflicting late events must neither overwrite a terminal result nor
	// occupy retry slots. Duplicate event receipts retain their existing path.
	for _, runnerID := range []string{runner.Id, "wrong-runner"} {
		eventID := publish(liveID, runnerID, "runner-error", map[string]any{"code": "COMMAND_EXPIRED", "message": "late terminal event", "retryable": false})
		message, err := subscription.NextMsg(5 * time.Second)
		require.NoError(t, err)
		var event hmessaging.EventEnvelope
		require.NoError(t, json.Unmarshal(message.Data, &event))
		assert.Equal(t, eventID, event.EventID)
	}
	result, err := dp.GetDeploymentWithResponse(t.Context(), orgID, liveID)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, result.StatusCode())
	assert.Equal(t, "succeeded", result.JSON200.Status)
}
