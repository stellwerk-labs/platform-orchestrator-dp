package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hnats"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	cpevents "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genevents"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/aws"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/cloud"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers/branchhandler"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers/createdephandler"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers/envdestroyhandler"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers/runnerresulthandler"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers/runnerstatushandler"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/middleware"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

const (
	mainConsumerDurable          = "platform-orchestrator-dp-main"
	runnerResultsConsumerDurable = "platform-orchestrator-dp-runner-results"
	runnerStatusConsumerDurable  = "platform-orchestrator-dp-runner-status"
)

type Worker struct {
	NATSConnection *nats.Conn
	JetStream      jetstream.JetStream
	DLQPublisher   hmessaging.Publisher
	EventPublisher hmessaging.Publisher
	Logger         *zap.Logger
	DB             model.Databaser
	OidcProvider   oidc.Provider
	HTTPClient     *http.Client

	ControlPlaneClient                 platformorchestratorcp.ClientWithResponsesInterface
	VaultClient                        vault.VaultClientInterface
	RunnerImage                        string
	RemoteRunnerCommandPublisher       runners.RemoteRunnerCommandPublisher
	RunnerLogsDeleter                  runners.RunnerLogsDeleter
	KubernetesRunnerPodSchedulingDelay time.Duration
	RunnerCommandTTL                   time.Duration
	RunnerNATSConfiguration            runners.RunnerNATSConfiguration
	RunnerBundleStore                  createdephandler.RunnerBundleStore
	MetadataOutputKey                  string
}

func (w *Worker) BuildRunnerStatusConsumer(ctx context.Context) (*hnats.Consumer, error) {
	k8sFactory := runners.NewClientFactory(w.HTTPClient, w.OidcProvider, w.Logger)
	awsTemporaryAuth := &cloud.AwsTemporaryCredsProvider{OidcProvider: w.OidcProvider, CredentialsClient: aws.NewCredentialsClient()}
	runnerFactory := runners.NewDefaultRunnerFactory(
		w.Logger,
		awsTemporaryAuth,
		k8sFactory,
		w.VaultClient,
		w.RunnerImage,
		w.MetadataOutputKey,
		w.RemoteRunnerCommandPublisher,
		w.KubernetesRunnerPodSchedulingDelay,
		w.RunnerCommandTTL,
		w.RunnerNATSConfiguration,
	)
	handler := runnerstatushandler.New(w.DB, w.EventPublisher, w.ControlPlaneClient, runnerFactory)
	consumer, err := hnats.EnsureDurableConsumer(ctx, w.JetStream, hnats.DurableConsumerConfig{
		Stream:         hmessaging.EventsStreamName,
		Durable:        runnerStatusConsumerDurable,
		FilterSubjects: []string{string(genevents.IoPlatformOrchestratorRunnerCheckStatus)},
		MaxDeliver:     runnerstatushandler.MaxDeliveries,
		AckWait:        MainConsumerAckWait,
		MaxAckPending:  RunnerStatusConcurrency,
	})
	if err != nil {
		return nil, err
	}
	return hnats.NewConsumer(consumer, func(ctx context.Context, delivery hmessaging.Delivery) error {
		return handler.Handle(ctx, w.Logger, delivery)
	}, hnats.ProcessingConfig{
		MaxDeliveries: runnerstatushandler.MaxDeliveries,
		DLQPublisher:  w.DLQPublisher,
		Logger:        w.Logger,
	})
}

func (w *Worker) BuildMainConsumer(ctx context.Context) (*hnats.Consumer, error) {
	k8sFactory := runners.NewClientFactory(w.HTTPClient, w.OidcProvider, w.Logger)
	awsTemporaryAuth := &cloud.AwsTemporaryCredsProvider{OidcProvider: w.OidcProvider, CredentialsClient: aws.NewCredentialsClient()}
	createDepHandler, err := createdephandler.Setup(
		w.DB,
		w.EventPublisher,
		w.ControlPlaneClient,
		awsTemporaryAuth,
		w.RunnerImage,
		w.VaultClient,
		w.MetadataOutputKey,
		w.Logger,
		k8sFactory,
		w.RemoteRunnerCommandPublisher,
		w.KubernetesRunnerPodSchedulingDelay,
		w.RunnerCommandTTL,
		w.RunnerNATSConfiguration,
		w.RunnerBundleStore,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to setup handler for deployment creation")
	}

	inner := handlers.Handler(&branchhandler.Handler{
		{PrefixPattern: envdestroyhandler.BranchPattern, Handler: envdestroyhandler.New(w.ControlPlaneClient, w.DB, w.EventPublisher, w.RunnerLogsDeleter)},
		{PrefixPattern: regexp.MustCompile(string(genevents.IoPlatformOrchestratorDeploymentCreated)), Handler: createDepHandler},
		{PrefixPattern: regexp.MustCompile(""), Handler: handlers.HandlerFunc(func(_ context.Context, logger *zap.Logger, _ hmessaging.Delivery) error {
			logger.Info("dropping unsupported message")
			return nil
		})},
	})
	observed := middleware.WrapWithObserver(inner, "main-consumer")
	consumer, err := hnats.EnsureDurableConsumer(ctx, w.JetStream, hnats.DurableConsumerConfig{
		Stream:  hmessaging.EventsStreamName,
		Durable: mainConsumerDurable,
		FilterSubjects: []string{
			string(genevents.IoPlatformOrchestratorDeploymentCreated),
			string(genevents.IoPlatformOrchestratorDeploymentUpdated),
			string(cpevents.IoPlatformOrchestratorEnvironmentUpdated),
		},
		MaxDeliver:    MainConsumerMaxDeliveries,
		AckWait:       MainConsumerAckWait,
		MaxAckPending: MainConsumerConcurrency,
	})
	if err != nil {
		return nil, err
	}
	return hnats.NewConsumer(consumer, func(ctx context.Context, delivery hmessaging.Delivery) error {
		return observed.Handle(ctx, w.Logger, delivery)
	}, hnats.ProcessingConfig{
		MaxDeliveries: MainConsumerMaxDeliveries,
		DLQPublisher:  w.DLQPublisher,
		Logger:        w.Logger,
	})
}

func (w *Worker) BuildRunnerResultsConsumer(
	ctx context.Context,
	applier runnerresulthandler.RunnerEventApplier,
) (*hnats.Consumer, error) {
	handler := &runnerresulthandler.Handler{Applier: applier}
	consumer, err := hnats.EnsureDurableConsumer(ctx, w.JetStream, hnats.DurableConsumerConfig{
		Stream:         hmessaging.RunnerEventsStreamName,
		Durable:        runnerResultsConsumerDurable,
		FilterSubjects: []string{runnerresulthandler.DeploymentResultSubjects, runnerresulthandler.RunnerErrorSubjects},
		MaxDeliver:     MainConsumerMaxDeliveries,
		AckWait:        MainConsumerAckWait,
		MaxAckPending:  MainConsumerConcurrency,
	})
	if err != nil {
		return nil, err
	}
	return hnats.NewConsumer(consumer, func(ctx context.Context, delivery hmessaging.Delivery) error {
		return handler.Handle(ctx, w.Logger, delivery)
	}, hnats.ProcessingConfig{
		MaxDeliveries: MainConsumerMaxDeliveries,
		DLQPublisher:  w.DLQPublisher,
		Logger:        w.Logger,
	})
}

// BuildCompletionsSubscription fans deployment completion notifications to
// every data-plane replica. Missing a notification is safe because the API
// waiter periodically rechecks PostgreSQL.
func (w *Worker) BuildCompletionsSubscription(
	hooks *completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}],
) (*nats.Subscription, error) {
	return w.NATSConnection.Subscribe(string(genevents.IoPlatformOrchestratorDeploymentUpdated), func(msg *nats.Msg) {
		var body events.CloudEvent[genevents.DeploymentChangedData]
		if err := json.Unmarshal(msg.Data, &body); err != nil {
			w.Logger.Error("failed to unmarshal deployment completion event", zap.Error(err))
			return
		}
		if body.Data.Status == nil || *body.Data.Status == string(model.DeploymentStatusExecuting) {
			return
		}
		n := hooks.Notify(completionhooks.DeploymentOrgAndId{
			OrgId:        body.Data.OrgId,
			DeploymentId: body.Data.DeploymentId.String(),
		}, struct{}{})
		w.Logger.Info("notified deployment complete", zap.Int("waiters", n))
	})
}
