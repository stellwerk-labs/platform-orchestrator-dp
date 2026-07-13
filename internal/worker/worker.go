package worker

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"regexp"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	delayqueues "github.com/stellwerk-labs/golib/hrabbitmq/delayqueues/v2"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	cpevents "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genevents"
	"github.com/wagslane/go-rabbitmq"
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
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers/runnerstatushandler"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/middleware"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/genevents"
)

type Worker struct {
	RabbitConn      *rabbitmq.Conn
	RabbitPublisher hrabbitmq.Publisher
	Logger          *zap.Logger
	DB              model.Databaser
	OidcProvider    oidc.Provider
	HttpClient      *http.Client
	Cache           *expirable.LRU[string, int32]

	DeploymentCompletedHooks           completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}]
	ControlPlaneClient                 platformorchestratorcp.ClientWithResponsesInterface
	VaultClient                        vault.VaultClientInterface
	RunnerImage                        string
	RunnerTokenSalt                    string
	ExternalDataplaneUrl               string
	InternalDataplaneHostname          string
	RunnerLogsSignedUrlGenerator       runners.RunnerLogsSignedUrlGenerator // GCS bucket for storing runner logs
	RunnerLogsDeleter                  runners.RunnerLogsDeleter
	KubernetesRunnerPodSchedulingDelay time.Duration
	MetadataOutputKey                  string
}

func (w *Worker) BuildMainConsumer() (*hrabbitmq.ConsumerWithHandlerWaiter, error) {
	// This branch handler runs each message routing key through a regex match from top to bottom and calls the handler that
	// first matches it. The final branch intentionally swallows the message as a success.
	k8sFactory := runners.NewClientFactory(w.HttpClient, w.OidcProvider, w.Logger)
	awsTemporaryAuth := &cloud.AwsTemporaryCredsProvider{OidcProvider: w.OidcProvider, CredentialsClient: aws.NewCredentialsClient()}
	createDepHandler, err := createdephandler.Setup(w.DB, w.RabbitPublisher, w.ControlPlaneClient, awsTemporaryAuth, w.RunnerImage, w.ExternalDataplaneUrl, w.VaultClient,
		w.RunnerTokenSalt, w.MetadataOutputKey, w.Logger, k8sFactory, w.InternalDataplaneHostname, w.RunnerLogsSignedUrlGenerator, w.KubernetesRunnerPodSchedulingDelay)
	if err != nil {
		return nil, errors.Wrap(err, "failed to setup handler for deployment creation")
	}

	runnerFactory := runners.NewDefaultRunnerFactory(
		w.Logger,
		awsTemporaryAuth,
		k8sFactory,
		w.VaultClient,
		w.ExternalDataplaneUrl,
		w.RunnerImage,
		w.RunnerTokenSalt,
		w.MetadataOutputKey,
		w.InternalDataplaneHostname,
		w.RunnerLogsSignedUrlGenerator,
		w.KubernetesRunnerPodSchedulingDelay,
	)
	runnerStatusHandler := runnerstatushandler.New(w.DB, w.RabbitPublisher, w.ControlPlaneClient, runnerFactory)

	// Branch handler will send the message through _every_ handler that matches the regex.
	var inner handlers.Handler = &branchhandler.Handler{
		{PrefixPattern: envdestroyhandler.BranchPattern, Handler: envdestroyhandler.New(w.ControlPlaneClient, w.DB, w.RabbitPublisher, w.RunnerLogsDeleter)},
		{PrefixPattern: regexp.MustCompile(string(genevents.IoPlatformOrchestratorDeploymentCreated)), Handler: createDepHandler},
		{PrefixPattern: regexp.MustCompile(string(genevents.IoPlatformOrchestratorRunnerCheckStatus)), Handler: runnerStatusHandler},
		{PrefixPattern: regexp.MustCompile(""), Handler: handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error {
			logger.Info("dropping unsupported message")
			return nil
		})},
	}

	// This middleware handles timeouts, panic recovery, graceful retries, and logging
	inner = middleware.WrapWithObserver(inner, "main-consumer", w.RabbitPublisher, w.Cache)

	return hrabbitmq.NewConsumerWithHandlerWaiter(
		w.RabbitConn,
		func(d rabbitmq.Delivery) (action rabbitmq.Action) {
			if err := inner.Handle(context.TODO(), w.Logger, &d); err != nil {
				return rabbitmq.NackDiscard
			}
			return rabbitmq.Ack
		},
		"platform-orchestrator-dp-main",
		rabbitmq.WithConsumerOptionsLogger(hrabbitmq.NewLogger(w.Logger)),
		rabbitmq.WithConsumerOptionsConsumerAutoAck(false),
		rabbitmq.WithConsumerOptionsConcurrency(MainConsumerConcurrency),
		rabbitmq.WithConsumerOptionsQueueDurable,
		rabbitmq.WithConsumerOptionsQueueArgs(rabbitmq.Table{
			"x-queue-type":              "quorum",
			"x-message-ttl":             MainConsumerMessageTtl.Milliseconds(),
			"x-dead-letter-exchange":    delayqueues.DefaultExchange,
			"x-dead-letter-routing-key": delayqueues.DeadLetterRoutingKey,
			// ensure we dead letter things correctly
			"x-dead-letter-strategy": "at-least-once",
			// ensure we reject publish if queue is full
			"x-overflow": "reject-publish",
		}),
		rabbitmq.WithConsumerOptionsExchangeName(events.DefaultExchange),

		rabbitmq.WithConsumerOptionsRoutingKey(string(genevents.IoPlatformOrchestratorDeploymentCreated)),
		rabbitmq.WithConsumerOptionsRoutingKey(string(genevents.IoPlatformOrchestratorDeploymentUpdated)),
		rabbitmq.WithConsumerOptionsRoutingKey(string(cpevents.IoPlatformOrchestratorEnvironmentUpdated)),
		rabbitmq.WithConsumerOptionsRoutingKey(string(genevents.IoPlatformOrchestratorRunnerCheckStatus)),
	)
}

func (w *Worker) BuildCompletionsConsumer(hooks *completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}]) (*hrabbitmq.ConsumerWithHandlerWaiter, error) {
	return hrabbitmq.NewConsumerWithHandlerWaiter(
		w.RabbitConn,
		func(d rabbitmq.Delivery) (action rabbitmq.Action) {
			var body events.CloudEvent[genevents.DeploymentChangedData]
			if err := json.Unmarshal(d.Body, &body); err != nil {
				w.Logger.Error("failed to unmarshal event body", zap.Error(err))
			} else if body.Data.Status != nil && *body.Data.Status != string(model.DeploymentStatusExecuting) {
				n := hooks.Notify(completionhooks.DeploymentOrgAndId{OrgId: body.Data.OrgId, DeploymentId: body.Data.DeploymentId.String()}, struct{}{})
				w.Logger.Info("notified deployment complete", zap.String("org_id", body.Data.OrgId), zap.String("deployment_id", body.Data.DeploymentId.String()), zap.Int("waiters", n))
			}
			return rabbitmq.Ack
		},
		"platform-orchestrator-dp-completions-"+rand.Text(),
		rabbitmq.WithConsumerOptionsLogger(hrabbitmq.NewLogger(w.Logger)),
		rabbitmq.WithConsumerOptionsConsumerAutoAck(true),
		rabbitmq.WithConsumerOptionsConcurrency(1),
		rabbitmq.WithConsumerOptionsQueueAutoDelete,
		rabbitmq.WithConsumerOptionsExchangeName(events.DefaultExchange),
		rabbitmq.WithConsumerOptionsRoutingKey(string(genevents.IoPlatformOrchestratorDeploymentUpdated)),
	)
}
