package createdephandler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/bundles"
	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/kubernetes"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/opt"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

// magicNoOpRunnerId is a temporary constant we use in testing to force the create-dep handler to retry all errors so
// that we can manually advance its behavior.
const magicNoOpRunnerId = "sunshine-weary-robin-runner"

type CreateDepHandler struct {
	db                 model.Databaser
	publisher          hmessaging.Publisher
	controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface
	runnerFactory      runners.RunnerFactory
	bundleStore        RunnerBundleStore
}

type RunnerBundleStore interface {
	Put(context.Context, jetstream.ObjectMeta, io.Reader) (*jetstream.ObjectInfo, error)
}

func Setup(
	db model.Databaser, pub hmessaging.Publisher, cp platformorchestratorcp.ClientWithResponsesInterface, awsTemporaryAuth runners.AwsTemporaryAuthProvider,
	rnImage string, vlt vault.VaultClientInterface,
	metadataOutputKey string, logger *zap.Logger, k8sClientBuilder kubernetes.ClientFactory,
	remoteRunnerCommandPublisher runners.RemoteRunnerCommandPublisher,
	kubernetesRunnerPodSchedulingDelay time.Duration,
	runnerCommandTTL time.Duration,
	runnerNATSConfiguration runners.RunnerNATSConfiguration,
	bundleStore RunnerBundleStore,
) (*CreateDepHandler, error) {
	return &CreateDepHandler{
		db:                 db,
		publisher:          pub,
		controlPlaneClient: cp,
		bundleStore:        bundleStore,
		runnerFactory: runners.NewDefaultRunnerFactory(
			logger,
			awsTemporaryAuth,
			k8sClientBuilder,
			vlt,
			rnImage,
			metadataOutputKey,
			remoteRunnerCommandPublisher,
			kubernetesRunnerPodSchedulingDelay,
			runnerCommandTTL,
			runnerNATSConfiguration,
		),
	}, nil
}

func (h *CreateDepHandler) Handle(ctx context.Context, logger *zap.Logger, delivery hmessaging.Delivery) error {
	var body events.CloudEvent[genevents.DeploymentChangedData]
	if err := json.Unmarshal(delivery.Data, &body); err != nil {
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
				} else if _, retry := hmessaging.AsRetryError(err); retry {
					return err
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

		messages, err := h.db.InsertPendingEventMessages(ctx, tx, []*hstandardoutbox.PendingEventMessage{
			{
				Subject: string(genevents.IoPlatformOrchestratorDeploymentUpdated),
				Payload: model.ConvertDeploymentToEventPayload(dep),
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
		return hmessaging.NewRetryError(errors.Wrap(err, "failed to make get-runner request"))
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
		logger.Info("job already running, ensuring status check is scheduled")
		return h.scheduleStatusCheck(ctx, d)
	}
	if h.bundleStore != nil {
		archive, err := bundles.Build(ctx, h.db, h.controlPlaneClient, d.OrgId, d.Id)
		if err != nil {
			return hmessaging.NewRetryError(errors.Wrap(err, "failed to build runner bundle"))
		}
		if _, err := h.bundleStore.Put(ctx, jetstream.ObjectMeta{Name: bundles.ObjectKey(d.OrgId, d.Id)}, archive); err != nil {
			return hmessaging.NewRetryError(errors.Wrap(err, "failed to store runner bundle in NATS Object Store"))
		}
	}

	if err := runner.Start(ctx); err != nil {
		if errors.Is(err, runners.ErrRunnerCommandPublishRetry) {
			return hmessaging.NewRetryError(errors.Wrap(err, "failed to durably publish runner command"))
		}
		if errors.Is(err, runners.ErrKubernetesAgentNotReachableRetry) {
			if time.Since(d.CreatedAt) < runners.KubernetesAgentConnectionIssueTolerance {
				return hmessaging.NewRetryError(errors.Wrap(err, "kubernetes agent temporary not reachable, will retry"))
			} else {
				return usererrors.NewUserErrorWithDetails(fmt.Sprintf("kubernetes-agent runner %q not reachable, please check your network connectivity and configuration", rn.Id), err)
			}
		}
		return err
	}

	return h.scheduleStatusCheck(ctx, d)
}

func (h *CreateDepHandler) scheduleStatusCheck(ctx context.Context, deployment *model.DeploymentSummary) error {
	statusCheckEvent := events.CloudEvent[genevents.RunnerStatusCheckData]{
		SpecVersion: events.CloudEventSpecVersion1{},
		Type:        genevents.EventType(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Time:        time.Now().UTC(),
		Data: genevents.RunnerStatusCheckData{
			DeploymentId: deployment.Id,
			OrgId:        deployment.OrgId,
			RunnerId:     deployment.RunnerId,
		},
	}
	payload, err := json.Marshal(statusCheckEvent)
	if err != nil {
		return errors.Wrap(err, "failed to marshal runner status check event")
	}
	if err := h.publisher.Publish(ctx, hmessaging.Message{
		ID:        deployment.Id.String() + ":runner-status-check",
		Subject:   string(genevents.IoPlatformOrchestratorRunnerCheckStatus),
		Data:      payload,
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		return hmessaging.NewRetryError(errors.Wrap(err, "failed to publish runner status check event"))
	}
	return nil
}
