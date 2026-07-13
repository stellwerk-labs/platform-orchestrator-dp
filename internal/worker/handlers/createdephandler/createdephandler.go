package createdephandler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	v2 "github.com/stellwerk-labs/golib/hrabbitmq/delayqueues/v2"
	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/zap"

	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/genevents"
)

// magicNoOpRunnerId is a temporary constant we use in testing to force the create-dep handler to retry all errors so
// that we can manually advance its behavior.
const magicNoOpRunnerId = "sunshine-weary-robin-runner"

type CreateDepHandler struct {
	db                 model.Databaser
	publisher          hrabbitmq.Publisher
	controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface
	runnerFactory      runners.RunnerFactory
	runnerTokenSalt    string
}

func Setup(
	db model.Databaser, pub hrabbitmq.Publisher, cp platformorchestratorcp.ClientWithResponsesInterface, awsTemporaryAuth runners.AwsTemporaryAuthProvider,
	rnImage, externalDataplaneUrl string, vlt vault.VaultClientInterface,
	deployTokenSalt, metadataOutputKey string, logger *zap.Logger, k8sClientBuilder kubernetes.ClientFactory,
	internalDataplaneHostname string, runnerLogsBucketSignedUrlGenerator runners.RunnerLogsSignedUrlGenerator, kubernetesRunnerPodSchedulingDelay time.Duration) (*CreateDepHandler, error) {
	return &CreateDepHandler{
		db:                 db,
		publisher:          pub,
		controlPlaneClient: cp,
		runnerFactory: runners.NewDefaultRunnerFactory(
			logger,
			awsTemporaryAuth,
			k8sClientBuilder,
			vlt,
			externalDataplaneUrl,
			rnImage,
			deployTokenSalt,
			metadataOutputKey,
			internalDataplaneHostname,
			runnerLogsBucketSignedUrlGenerator,
			kubernetesRunnerPodSchedulingDelay,
		),
	}, nil
}

func (h *CreateDepHandler) Handle(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error {
	var body events.CloudEvent[genevents.DeploymentChangedData]
	if err := json.Unmarshal(d.Body, &body); err != nil {
		return errors.Wrap(err, "failed to unmarshal event body")
	} else {
		ids, ctx := hlogger.EnsurePlatformOrchestratorIdsOnCtx(ctx)
		ids.OrgId = body.Data.OrgId
		ids.ProjectId = body.Data.ProjectId
		ids.EnvId = body.Data.EnvId
		ids.DeployId = body.Data.DeploymentId.String()
		logger = hlogger.TraceScopedLoggerFromCtx(logger, ctx).WithLazy(ids.AsLogField())

		logger.Sugar().Info("handling deployment created event")
		if d, _, _, _, err := h.db.GetDeployment(ctx, nil, body.Data.OrgId, body.Data.DeploymentId, model.GetModeDefault); err != nil {
			if _, ok := model.IsErrNotFound(err); ok {
				logger.Info("discarding event message belonging to deployment that is now gone")
				return nil
			}
			return err
		} else if d.RunnerId == "" {
			return errors.New("runner not defined for deployment")
		} else {
			ids.RunnerId = d.RunnerId
			if body.Data.Revision > 0 && d.Revision != body.Data.Revision {
				logger.Info("discarding event message belonging to old deployment revision")
				return nil
			} else if d.Status != model.DeploymentStatusExecuting {
				logger.Info("discarding event message belonging to deployment that is not executing")
				return nil
			}

			if err := h.handleInner(ctx, logger, d); err != nil {
				if d.RunnerId == magicNoOpRunnerId {
					return err
				} else if gre := (v2.GracefulRetryError)(nil); errors.As(err, &gre) {
					return gre
				} else if ue := new(usererrors.UserError); errors.As(err, &ue) {
					logger.Warn("failing deployment due to user error", zap.Error(ue), zap.Any("details", ue.Details))
					return h.failDeployment(ctx, logger, d, ue.Error())
				}
				logger.Error("failing deployment due to unexpected error", zap.Error(err))
				return h.failDeployment(ctx, logger, d, "internal failure - please contact support")
			}

			return nil
		}
	}
}

func (h *CreateDepHandler) failDeployment(ctx context.Context, logger *zap.Logger, dep *model.DeploymentSummary, statusMessage string) error {
	if tx, err := h.db.BeginTx(ctx, nil); err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	} else {
		defer func() {
			if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
				logger.Error("failed to rollback transaction", zap.Error(err))
			}
		}()

		if dep, err = h.db.UpdateDeploymentStatusAndOutputs(ctx, tx, dep.Id, model.UpdateDeploymentStatusAndOutputsParams{
			Status:           model.DeploymentStatusFailed,
			StatusMessage:    statusMessage,
			Metrics:          dep.Metrics,
			ExpectedRevision: opt.Of(int64(dep.Revision)),
		}); err != nil {
			return errors.Wrap(err, "failed to update deployment status")
		}

		if err := h.db.CreateDeploymentHistoryRecord(ctx, tx, dep); err != nil {
			return errors.Wrap(err, "failed to create deployment history record")
		}

		messages, err := h.db.InsertPendingEventMessages(ctx, tx, []*hstandardreliableoutbox.PendingEventMessage{
			{
				Exchange:   events.DefaultExchange,
				RoutingKey: string(genevents.IoPlatformOrchestratorDeploymentUpdated),
				Payload:    model.ConvertDeploymentToEventPayload(dep),
			},
		})
		if err != nil {
			return errors.Wrap(err, "failed to insert pending event messages")
		}
		if err := tx.Commit(); err != nil {
			return errors.Wrap(err, "failed to commit transaction")
		}
		logger.Info(
			"deployment completed",
			zap.String("status", string(model.DeploymentStatusFailed)),
			zap.String("status_message", statusMessage))
		reliableoutbox.OptimisticPublish(ctx, logger, h.db.AsReliableOutboxStore(), h.publisher, messages)
		return nil
	}
}

func (h *CreateDepHandler) handleInner(ctx context.Context, logger *zap.Logger, d *model.DeploymentSummary) error {
	res, err := h.controlPlaneClient.GetInternalRunnerWithResponse(ctx, d.OrgId, d.RunnerId)
	if err != nil {
		return v2.NewGracefulRetryError(errors.Wrap(err, "failed to make get-runner request"))
	} else if res.StatusCode() == http.StatusNotFound {
		return usererrors.NewUserError(fmt.Sprintf("no runner has been configured with id '%s'", d.RunnerId))
	} else if res.StatusCode() != http.StatusOK {
		return errors.Errorf("unexpected status code when getting runner '%s': %s: %s", d.RunnerId, res.Status(), string(res.Body))
	}
	rn := *res.JSON200
	runner, err := h.runnerFactory.CreateRunner(ctx, rn, d)
	if err != nil {
		return errors.Wrap(err, "failed to create runner")
	}

	if isRunning, err := runner.IsRunning(ctx); err != nil {
		return errors.Wrap(err, "failed to check if runner is running")
	} else if isRunning {
		logger.Info("job already running, skipping")
		return nil
	}

	if err := runner.Start(ctx); err != nil {
		if errors.Is(err, runners.ErrKubernetesAgentNotReachableRetry) {
			if time.Since(d.CreatedAt) < runners.KubernetesAgentConnectionIssueTolerance {
				return v2.NewGracefulRetryError(errors.Wrap(err, "kubernetes agent temporary not reachable, will retry"))
			} else {
				return usererrors.NewUserErrorWithDetails(fmt.Sprintf("kubernetes-agent runner %q not reachable, please check your network connectivity and configuration", rn.Id), err)
			}
		}
		return err
	}

	if err := h.scheduleStatusCheck(ctx, d); err != nil {
		logger.Warn("failed to schedule initial status check", zap.Error(err))
	}

	return nil
}

func (h *CreateDepHandler) scheduleStatusCheck(ctx context.Context, d *model.DeploymentSummary) error {
	statusCheckEvent := events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now(),
		Data: genevents.RunnerStatusCheckData{
			DeploymentId: d.Id,
			OrgId:        d.OrgId,
			RunnerId:     d.RunnerId,
		},
	}

	payload, err := json.Marshal(statusCheckEvent)
	if err != nil {
		return errors.Wrap(err, "failed to marshal status check event")
	}

	// Schedule initial status check using reliable outbox (will be processed immediately, then the handler will manage subsequent delays)
	if confirms, err := h.publisher.PublishWithDeferredConfirmWithContext(
		ctx, payload, []string{string(genevents.IoPlatformOrchestratorRunnerCheckStatus)},
		rabbitmq.WithPublishOptionsExchange(events.DefaultExchange),
		rabbitmq.WithPublishOptionsContentType("application/json"),
		rabbitmq.WithPublishOptionsContentEncoding("utf-8"),
		rabbitmq.WithPublishOptionsTimestamp(time.Now().UTC()),
	); err != nil {
		return errors.Wrap(err, "rabbit publish failed")
	} else {
		for _, confirm := range confirms {
			if ok, err := confirm.WaitContext(ctx); err != nil {
				return errors.Wrap(err, "rabbit confirm timed out")
			} else if !ok {
				return errors.Wrap(err, "rabbit ping confirm failed")
			}
		}
	}
	return nil
}
