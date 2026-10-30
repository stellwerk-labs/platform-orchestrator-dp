package deploymentadmissionhandler

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hmessaging"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v3/genevents"
)

// DeploymentQueue promotes an Environment's queued Deployments.
type DeploymentQueue interface {
	PromoteDeploymentQueue(ctx context.Context, environmentUUID uuid.UUID) error
}

type Handler struct {
	Admissions *api.OrdinaryDeploymentAdmissionService
	Queue      DeploymentQueue
	Next       handlers.Handler
}

func (h Handler) Handle(ctx context.Context, logger *zap.Logger, delivery hmessaging.Delivery) error {
	if delivery.Subject == string(genevents.IoPlatformOrchestratorDeploymentUpdated) && (h.Admissions != nil || h.Queue != nil) {
		var event events.CloudEvent[genevents.DeploymentChangedData]
		if err := json.Unmarshal(delivery.Data, &event); err != nil {
			return errors.Wrap(err, "failed to decode Deployment admission completion event")
		}
		if h.Admissions != nil {
			if err := h.Admissions.SettleDeployment(ctx, event.Data.DeploymentId); err != nil {
				return errors.Wrap(err, "failed to settle Core Deployment admission")
			}
		}
		// The lease is released and Core settled the admission guard, so the
		// Environment's next queued Deployment can start now.
		if h.Queue != nil && event.Data.Status != nil && *event.Data.Status != string(model.DeploymentStatusExecuting) {
			if err := h.Queue.PromoteDeploymentQueue(ctx, event.Data.EnvUuid); err != nil {
				logger.Warn("failed to promote the deployment queue; the sweeper retries", zap.Error(err))
			}
		}
	}
	return h.Next.Handle(ctx, logger, delivery)
}
