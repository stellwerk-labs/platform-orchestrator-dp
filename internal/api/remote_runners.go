package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"
)

var waitForRunnerMessagesTimeoutTime = time.Second * 30

// RemoteRunnerMessageResponse wraps the generated response type to provide proper JSON marshaling
type RemoteRunnerMessageResponse struct {
	RemoteRunnerMessage
}

func (r RemoteRunnerMessageResponse) MarshalJSON() ([]byte, error) {
	return r.RemoteRunnerMessage.MarshalJSON()
}

// VisitWaitForRemoteRunnerMessagesResponse implements the response interface
func (r RemoteRunnerMessageResponse) VisitWaitForRemoteRunnerMessagesResponse(w http.ResponseWriter) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(r)
}

// Long-poll wait for pending messages for a remote runner
// (POST /orgs/{orgId}/runners/{runnerId}/actions/poll-requests)
func (s *Server) WaitForRemoteRunnerMessages(ctx context.Context, request WaitForRemoteRunnerMessagesRequestObject) (WaitForRemoteRunnerMessagesResponseObject, error) {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.RunnerId = request.RunnerId
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).WithLazy(ids.AsLogField())

	// Check if org and runner exist
	if res, err := s.ControlPlaneClient.GetRunnerWithResponse(ctx, request.OrgId, request.RunnerId); err != nil {
		return nil, errors.Wrap(err, "failed to get runner")
	} else if res.StatusCode() == http.StatusNotFound {
		logger.Warn("Runner not found")
		return WaitForRemoteRunnerMessages404JSONResponse{Generate404Response(res.JSON404.Message)}, nil
	} else if res.StatusCode() != http.StatusOK {
		return nil, errors.Wrapf(errors.New("unexpected status code"), "getting runner: %d, body: %s", res.StatusCode(), string(res.Body))
	}

	ch, fin := s.RemoteRunnerCompletedHooks.AddWaiter(completionhooks.RunnerAndOrgId{
		RunnerId: request.RunnerId,
		OrgId:    request.OrgId,
	})
	logger.Debug("Waiting for messages from remote runner")

	ctx, cancel := context.WithTimeout(ctx, waitForRunnerMessagesTimeoutTime)
	defer cancel()

	defer fin()
	select {
	case msg, ok := <-ch:
		var resp = new(RemoteRunnerMessage)
		if ok {
			switch msg.Action {
			case completionhooks.WaitForRunnerMessagesActionCreateJob:
				_ = resp.FromRemoteRunnerMessageCreateJob(RemoteRunnerMessageCreateJob{
					Action:          CreateJob,
					JobId:           msg.JobId,
					Namespace:       msg.Namespace,
					Configuration:   msg.Configuration,
					DeploymentToken: msg.DeploymentToken,
				})
			case completionhooks.WaitForRunnerMessagesActionCheckJobStatus:
				_ = resp.FromRemoteRunnerMessageCheckJobStatus(RemoteRunnerMessageCheckJobStatus{
					Action:          CheckJobStatus,
					JobId:           msg.JobId,
					Namespace:       msg.Namespace,
					DeploymentToken: msg.DeploymentToken,
					ExpiresAt:       msg.ExpiresAt,
				})
			default:
				logger.Warn("Received unknown action from runner", zap.String("runner_action", string(msg.Action)))
				return WaitForRemoteRunnerMessages204Response{}, nil
			}
			logger.Debug("Received message for remote runner", zap.String("runner_action", string(msg.Action)))
			return RemoteRunnerMessageResponse{RemoteRunnerMessage: *resp}, nil
		} else {
			logger.Warn("Channel closed before receiving message")
			return WaitForRemoteRunnerMessages204Response{}, nil
		}
	case <-ctx.Done():
		return WaitForRemoteRunnerMessages204Response{}, nil
	}
}

// Push a message to a remote runner
// (POST /internal/orgs/{orgId}/runners/{runnerId}/push-message)
func (s *Server) InternalPushMessageToRemoteRunner(ctx context.Context, request InternalPushMessageToRemoteRunnerRequestObject) (InternalPushMessageToRemoteRunnerResponseObject, error) {
	ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
	ids.RunnerId = request.RunnerId
	logger := hlogger.TraceScopedLoggerFromCtx(s.Logger, ctx).WithLazy(ids.AsLogField())

	var msg completionhooks.RunnerMessage
	body, err := request.Body.ValueByDiscriminator()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get message struct by request body discriminator")
	}

	switch body := body.(type) {
	case RemoteRunnerMessageCreateJob:
		msg = completionhooks.RunnerMessage{
			Action:          completionhooks.WaitForRunnerMessagesActionCreateJob,
			JobId:           body.JobId,
			Namespace:       body.Namespace,
			Configuration:   body.Configuration,
			DeploymentToken: body.DeploymentToken,
		}
	case RemoteRunnerMessageCheckJobStatus:
		msg = completionhooks.RunnerMessage{
			Action:          completionhooks.WaitForRunnerMessagesActionCheckJobStatus,
			JobId:           body.JobId,
			Namespace:       body.Namespace,
			DeploymentToken: body.DeploymentToken,
			ExpiresAt:       body.ExpiresAt,
		}
	default:
		logger.Warn("Received unknown body type from request")
		return nil, errors.New("unknown body type received in request")
	}
	if notified := s.RemoteRunnerCompletedHooks.Notify(completionhooks.RunnerAndOrgId{
		RunnerId: request.RunnerId,
		OrgId:    request.OrgId,
	}, msg); notified == 0 {
		return InternalPushMessageToRemoteRunner503Response{}, nil
	} else {
		return InternalPushMessageToRemoteRunner204Response{}, nil
	}
}
