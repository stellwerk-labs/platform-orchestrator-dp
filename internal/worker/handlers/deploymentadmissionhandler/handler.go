package deploymentadmissionhandler

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hmessaging"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

type Handler struct {
	Admissions *api.OrdinaryDeploymentAdmissionService
	Next       handlers.Handler
}

func (h Handler) Handle(ctx context.Context, logger *zap.Logger, delivery hmessaging.Delivery) error {
	if h.Admissions != nil && delivery.Subject == string(genevents.IoPlatformOrchestratorDeploymentUpdated) {
		var event events.CloudEvent[genevents.DeploymentChangedData]
		if err := json.Unmarshal(delivery.Data, &event); err != nil {
			return errors.Wrap(err, "failed to decode Deployment admission completion event")
		}
		if err := h.Admissions.SettleDeployment(ctx, event.Data.DeploymentId); err != nil {
			return errors.Wrap(err, "failed to settle Core Deployment admission")
		}
	}
	return h.Next.Handle(ctx, logger, delivery)
}
