package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"

	"github.com/golang/mock/gomock"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"
)

const (
	orgId    = "test-org"
	runnerId = "test-runner"
)

func TestWaitForRemoteRunnerMessages_event_trigger(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().
		GetRunnerWithResponse(gomock.Any(), orgId, runnerId).
		Return(&platformorchestratorcp.GetRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.Runner{
				Id:    runnerId,
				OrgId: orgId,
			},
		}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var createJobMessage = completionhooks.RunnerMessage{
		Action:        completionhooks.WaitForRunnerMessagesActionCreateJob,
		JobId:         "test-job-id",
		Namespace:     "test-namespace",
		Configuration: map[string]interface{}{"key": "value"},
	}

	msg := new(RemoteRunnerMessage)
	require.NoError(t, msg.FromRemoteRunnerMessageCreateJob(RemoteRunnerMessageCreateJob{
		Action:        CreateJob,
		JobId:         createJobMessage.JobId,
		Namespace:     createJobMessage.Namespace,
		Configuration: createJobMessage.Configuration,
	}))

	go func() {
		time.Sleep(2 * time.Second)
		_, err := s.InternalPushMessageToRemoteRunner(ctx, InternalPushMessageToRemoteRunnerRequestObject{
			OrgId:    orgId,
			RunnerId: runnerId,
			Body:     msg,
		})
		assert.NoError(t, err)
	}()

	r, err := s.WaitForRemoteRunnerMessages(ctx, WaitForRemoteRunnerMessagesRequestObject{
		OrgId:    orgId,
		RunnerId: runnerId,
	})

	if assert.NoError(t, err) {
		r200, ok := r.(RemoteRunnerMessageResponse)
		assert.True(t, ok)
		assert.NotNil(t, r200.RemoteRunnerMessage)

		// Parse the union field to check if it's a RemoteRunnerMessageCreateJob
		createJob, err := r200.AsRemoteRunnerMessageCreateJob()
		if assert.NoError(t, err) {
			assert.Equal(t, RemoteRunnerMessageCreateJob{
				Action:        RemoteRunnerMessageAction(createJobMessage.Action),
				JobId:         createJobMessage.JobId,
				Namespace:     createJobMessage.Namespace,
				Configuration: createJobMessage.Configuration,
			}, createJob)
		}
	}
}

func TestWaitForRemoteRunnerMessages_runner_not_found(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().
		GetRunnerWithResponse(gomock.Any(), orgId, runnerId).
		Return(&platformorchestratorcp.GetRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
			JSON404:      &platformorchestratorcp.Error{Message: "Runner not found"},
		}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	r, err := s.WaitForRemoteRunnerMessages(ctx, WaitForRemoteRunnerMessagesRequestObject{
		OrgId:    orgId,
		RunnerId: runnerId,
	})

	if assert.NoError(t, err) {
		r404, ok := r.(WaitForRemoteRunnerMessages404JSONResponse)
		assert.True(t, ok)
		assert.Equal(t, "Runner not found", r404.Message)
	}
}

func TestWaitForRemoteRunnerMessages_event_trigger_check_job_status(t *testing.T) {
	_, s, fin := MockServer(t)
	defer fin()
	cpClient := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
	cpClient.EXPECT().
		GetRunnerWithResponse(gomock.Any(), orgId, runnerId).
		Return(&platformorchestratorcp.GetRunnerResponse{
			HTTPResponse: &http.Response{StatusCode: http.StatusOK},
			JSON200: &platformorchestratorcp.Runner{
				Id:    runnerId,
				OrgId: orgId,
			},
		}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var checkJobStatusMessage = completionhooks.RunnerMessage{
		Action:    completionhooks.WaitForRunnerMessagesActionCheckJobStatus,
		JobId:     "test-job-id",
		Namespace: "test-namespace",
		ExpiresAt: time.Now().Add(15 * time.Second),
	}

	msg := new(RemoteRunnerMessage)
	require.NoError(t, msg.FromRemoteRunnerMessageCheckJobStatus(RemoteRunnerMessageCheckJobStatus{
		Action:    CheckJobStatus,
		JobId:     checkJobStatusMessage.JobId,
		Namespace: checkJobStatusMessage.Namespace,
		ExpiresAt: checkJobStatusMessage.ExpiresAt,
	}))

	go func() {
		time.Sleep(2 * time.Second)
		_, err := s.InternalPushMessageToRemoteRunner(ctx, InternalPushMessageToRemoteRunnerRequestObject{
			OrgId:    orgId,
			RunnerId: runnerId,
			Body:     msg,
		})
		assert.NoError(t, err)
	}()

	r, err := s.WaitForRemoteRunnerMessages(ctx, WaitForRemoteRunnerMessagesRequestObject{
		OrgId:    orgId,
		RunnerId: runnerId,
	})

	if assert.NoError(t, err) {
		r200, ok := r.(RemoteRunnerMessageResponse)
		assert.True(t, ok)
		assert.NotNil(t, r200.RemoteRunnerMessage)

		// Parse the union field to check if it's a RemoteRunnerMessageCreateJob
		checkJobStatusMsg, err := r200.AsRemoteRunnerMessageCheckJobStatus()
		if assert.NoError(t, err) {
			require.NotZero(t, checkJobStatusMsg.ExpiresAt)
			checkJobStatusMsg.ExpiresAt = time.Time{} // zero it out for comparison
			assert.Equal(t, RemoteRunnerMessageCheckJobStatus{
				Action:    RemoteRunnerMessageAction(checkJobStatusMessage.Action),
				JobId:     checkJobStatusMessage.JobId,
				Namespace: checkJobStatusMessage.Namespace,
			}, checkJobStatusMsg)
		}
	}
}
