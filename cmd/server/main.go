package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hconfig"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	delayqueues "github.com/stellwerk-labs/golib/hrabbitmq/delayqueues/v2"
	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	"github.com/stellwerk-labs/golib/hretrier"
	"github.com/stellwerk-labs/golib/hstandardreliableoutbox"
	"github.com/stellwerk-labs/golib/hvaultapi"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/storage"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/config"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/events"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"

	vaultapi "github.com/hashicorp/vault/api"
	htelemetry "github.com/stellwerk-labs/golib/htelemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	rabbitmqCacheSize      = 10000
	rabbitmqCacheTtl       = 30 * time.Minute
	concurrentLogDeletes   = 20
	runtimeMetricsInterval = 5 * time.Second
	samplingRatio          = 1
)

var buildInfo *debug.BuildInfo

func init() {
	buildInfo, _ = debug.ReadBuildInfo()
}

func main() {
	logw, err := hlogger.NewHLogger("INFO", false, "json")
	if err != nil {
		log.Fatalf("Error building logger: %v (%s %s)", err, path.Base(buildInfo.Main.Path), buildInfo.Main.Version)
	}
	defer hlogger.OnExit(logw.Logger)
	zap.ReplaceGlobals(logw.Logger)
	zap.L().Info("Starting", zap.String("app", path.Base(buildInfo.Main.Path)), zap.String("version", buildInfo.Main.Version))

	cfg := new(config.Configuration)
	if err := hconfig.LoadConfigWithoutRetag(cfg); err != nil {
		zap.L().Fatal("failed to load config", zap.Error(err))
	}

	if err := logw.ChangeLevel(cfg.LogLevel); err != nil {
		zap.L().Fatal("error setting log level", zap.Error(err))
	}

	ctx := context.Background()
	if cfg.OTELEnabled {
		_, shutdown, err := htelemetry.StartOTel(ctx, htelemetry.OTelConfig{
			ServiceName:    path.Base(buildInfo.Main.Path),
			ServiceVersion: buildInfo.Main.Version,
			Logger:         zap.L(),

			// Custom TracerProvider options (e.g., sampling)
			TracerProviderOptions: []sdktrace.TracerProviderOption{
				sdktrace.WithSampler(sdktrace.TraceIDRatioBased(samplingRatio)),
			},
			RuntimeMetrics:         ref.Ref(true),
			RuntimeMetricsInterval: runtimeMetricsInterval,
		})

		if err != nil {
			zap.L().Fatal("failed to start otel tracing", zap.Error(err))
		}
		defer func() {
			if err := shutdown(ctx); err != nil {
				zap.L().Error("failed to shutdown otel tracing", zap.Error(err))
			}
		}()
	}

	http.DefaultClient = hretrier.WrapHttpClientWithStandardRetries(http.DefaultClient)

	cpClient, err := platformorchestratorcp.NewClientWithResponses(
		cfg.ControlPlaneUrl,
		platformorchestratorcp.WithHTTPClient(http.DefaultClient),
		platformorchestratorcp.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("From", userid.InternalSystemUuid.String())
			return nil
		}),
	)
	if err != nil {
		zap.L().Fatal("Failed to initialize control plane client", zap.Error(err))
	}

	iamClient, err := platformorchestratoriam.NewClientWithResponses(
		cfg.IamUrl,
		platformorchestratoriam.WithHTTPClient(http.DefaultClient),
		platformorchestratoriam.WithRequestEditorFn(func(ctx context.Context, req *http.Request) error {
			req.Header.Set("From", userid.InternalSystemUuid.String())
			return nil
		}),
	)
	if err != nil {
		zap.L().Fatal("Failed to initialize iam client", zap.Error(err))
	}

	dbConnStr := fmt.Sprintf(
		"dbname=%s user=%s password=%s host=%s port=%s connect_timeout=5 sslmode=disable",
		cfg.DatabaseName, cfg.DatabaseUser, cfg.DatabasePassword, cfg.DatabaseHost, cfg.DatabasePort)
	db, err := model.NewDatabaser(context.Background(), zap.L(), dbConnStr)
	if err != nil {
		zap.L().Fatal("Failed to initialize database", zap.Error(err))
	}
	defer func() {
		zap.L().Info("Closing database")
		if err := db.Close(); err != nil {
			zap.L().Error("failed to close database", zap.Error(err))
		}
		zap.L().Info("Database closed")
	}()

	amqpConnectionString, err := cfg.GetAmqpConnectionString()
	if err != nil {
		zap.L().Fatal("Failed to get AMQP connection string", zap.Error(err))
	}
	conn, err := rabbitmq.NewConn(amqpConnectionString, rabbitmq.WithConnectionOptionsLogger(hrabbitmq.NewLogger(zap.L())))
	if err != nil {
		zap.L().Fatal("Failed to initialize rabbitmq connection", zap.Error(err))
	}
	defer func() {
		if err := conn.Close(); err != nil {
			zap.L().Error("Failed to close connection", zap.Error(err))
		}
	}()

	publisher, err := rabbitmq.NewPublisher(
		conn, rabbitmq.WithPublisherOptionsLogger(hrabbitmq.NewLogger(zap.L())),
		rabbitmq.WithPublisherOptionsExchangeName(events.DefaultExchange),
		rabbitmq.WithPublisherOptionsExchangeDeclare,
		rabbitmq.WithPublisherOptionsExchangeDurable,
		rabbitmq.WithPublisherOptionsExchangeKind("topic"),
		rabbitmq.WithPublisherOptionsLogger(hrabbitmq.NewLogger(zap.L())),
	)
	if err != nil {
		zap.L().Fatal("Failed to initialize rabbitmq publisher", zap.Error(err))
	}
	defer publisher.Close()
	publisher.NotifyPublish(func(p rabbitmq.Confirmation) {
		zap.L().Debug("message publish confirmation received", zap.Bool("ack", p.Ack))
	})

	delayqueues.DefaultExchange = events.DefaultExchange
	// We need to distinguish our outbox messages from those produced by the CP or other components in our system so
	// that components can deduplicate messages using the message id. So we tack on a prefix.
	hstandardreliableoutbox.MessageIdPrefix = "platform-orchestrator-dp-"

	vaultHttpClient := &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
	hvaultapiClient, err := hvaultapi.NewWithDefaults(cfg.VaultURL, cfg.VaultAuth, cfg.VaultRole, vaultHttpClient, zap.L(), func(config *vaultapi.Config) {
		config.MaxRetries = 2
	})
	if err != nil {
		zap.L().Fatal("Failed to initialize vault client", zap.Error(err))
	}

	hvaultapiClient.WaitUntilReady(ctx)
	go hvaultapiClient.PeriodicallyRenewToken(ctx)
	vlt := vault.NewVaultClient(hvaultapiClient.Client(), zap.L())

	server := api.Server{
		RabbitMqPublisher:          publisher,
		Database:                   db,
		Logger:                     zap.L(),
		ControlPlaneClient:         cpClient,
		RunnerTokenSalt:            cfg.RunnerTokenSalt,
		DeploymentCompletedHooks:   &completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}]{MaximumWaitersPerHandle: completionhooks.MaximumWaitersPerDeploymentHandler},
		Vault:                      vlt,
		OidcIssuerUrl:              cfg.OidcIssuerUrl,
		RemoteRunnerCompletedHooks: &completionhooks.CompletionHooks[completionhooks.RunnerAndOrgId, completionhooks.RunnerMessage]{},
		IamClient:                  iamClient,
	}
	echo, err := hecho.DefaultEchoServerWithValidation(&hecho.ValidatedServerConfig{
		AppName:          path.Base(buildInfo.Main.Path),
		Logger:           server.Logger,
		OpenAPIRawSchema: api.MustDecodeOpenApiSpec(),
		Tracing:          hecho.TracingOTel,
		OpenAPISkipperFn: func(c echo.Context) bool {
			return c.Path() == "/alive" || c.Path() == "/health" || c.Path() == "/internal/actions/flush-pending-messages"
		},
	})
	if err != nil {
		zap.L().Fatal("Failed to setup server with schema validation", zap.Error(err))
	}

	echo.Use(middleware.EchoCtxMiddleware)
	echo.Use(echomiddleware.RequestID())

	bgCtx, bgCancel := context.WithCancel(ctx)
	defer bgCancel()
	go func() {
		zap.L().Info("Starting scheduled flush of pending messages")
		reliableoutbox.ScheduledFlushPendingMessages(bgCtx, server.Database.AsReliableOutboxStore(), publisher, reliableoutbox.DefaultScheduledFlushPeriodFunc)
		zap.L().Info("Stopped scheduled flush of pending messages")
	}()

	server.MapRoutes(echo)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownDelay)
		defer cancel()

		// Gracefully shutdown the server by waiting on existing requests (except websockets).
		zap.L().Info("Gracefully shutting down webserver")
		if err := echo.Shutdown(ctx); err != nil {
			zap.L().Error("failed to gracefully shutdown webserver", zap.Error(err))
			if err := echo.Close(); err != nil {
				zap.L().Error("Failed to terminate the echo server", zap.Error(err))
			}
		} else {
			zap.L().Info("webserver shutdown")
		}
	}()

	// This cache is used by hrabbitmq library for messages de-duplication
	queueCache := expirable.NewLRU[string, int32](rabbitmqCacheSize, nil, rabbitmqCacheTtl)

	// Set up the delay queues which expire the messages after the N seconds delays and then sends the message back to the common exchange
	if err := delayqueues.SetupStandardDelayConsumers(conn, zap.L().With(zap.String("consumer", "delay"))); err != nil {
		zap.L().Fatal("Failed to setup delay queues", zap.Error(err))
	}

	// Set up the dead letter queue and consumer which pushes the messages onto the delay queues and exponential backoff.
	dlc, err := delayqueues.SetupStandardDeadLetterConsumer(conn, zap.L().With(zap.String("consumer", "dead-letters")), publisher, queueCache)
	if err != nil {
		zap.L().Fatal("Failed to setup dead letter queue", zap.Error(err))
	}
	defer func() {
		zap.L().Info("Shutting down dead letter queue consumer")
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		dlc.CloseWithContext(ctx)
		zap.L().Info("Dead letter queue consumer shutdown")
	}()

	storageClient, err := storage.NewStorageClient(ctx, cfg.RunnerLogsBucketEndpoint, cfg.RunnerLogsBucketCreds, server.Logger)
	if err != nil {
		zap.L().Fatal("Failed to create storage client", zap.Error(err))
	}

	server.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		return storageClient.GetReader(ctx, cfg.RunnerLogsBucket, filename)
	}

	var oidcProvider oidc.Provider
	if cfg.OidcIssuerUrl != "" {
		oidcProvider = oidc.NewProvider(cfg.OidcIssuerUrl, vlt, oidc.ProviderOptions{})
	}

	wrk := &worker.Worker{
		RabbitConn: conn, RabbitPublisher: publisher, DB: db,
		Logger: zap.L().Named("worker"), ControlPlaneClient: cpClient,
		VaultClient:               vlt,
		RunnerTokenSalt:           cfg.RunnerTokenSalt,
		RunnerImage:               cfg.RunnerImage,
		ExternalDataplaneUrl:      cfg.ExternalDataplaneUrl,
		OidcProvider:              oidcProvider,
		HttpClient:                http.DefaultClient,
		Cache:                     queueCache,
		InternalDataplaneHostname: cfg.InternalDataplaneHostname,
		RunnerLogsSignedUrlGenerator: func(ctx context.Context, deploymentUuid, encryptedLogsRecipient string) (string, error) {
			if encryptedLogsRecipient == "" {
				return "", nil
			}
			return storageClient.GetPresignedURL(ctx, cfg.RunnerLogsBucket, deploymentUuid, cfg.RunnerLogsSignedUrlExpirationTime)
		},
		RunnerLogsDeleter: func(ctx context.Context, envUuid string) error {
			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(concurrentLogDeletes)

			it := storageClient.ListObjects(gctx, cfg.RunnerLogsBucket, envUuid+"/")
			for {
				objectName, err := it.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					return errors.Wrap(err, "failed to list runner logs objects for deletion")
				}
				g.Go(func() error {
					return storageClient.DeleteObject(gctx, cfg.RunnerLogsBucket, objectName)
				})
			}
			return g.Wait()
		},
		KubernetesRunnerPodSchedulingDelay: cfg.KubernetesRunnerPodSchedulingDelay,
		MetadataOutputKey:                  cfg.MetadataOutputKey,
	}
	wrkc, err := wrk.BuildMainConsumer()
	if err != nil {
		zap.L().Fatal("failed to setup main consumer", zap.Error(err))
	}
	defer func() {
		zap.L().Info("Shutting down main consumer")
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		wrkc.CloseWithContext(ctx)
		zap.L().Info("Main consumer shutdown")
	}()

	chc, err := wrk.BuildCompletionsConsumer(server.DeploymentCompletedHooks)
	if err != nil {
		zap.L().Fatal("failed to setup completions consumer", zap.Error(err))
	}
	defer func() {
		zap.L().Info("Shutting down completions consumer")
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		chc.CloseWithContext(ctx)
		zap.L().Info("completions consumer shutdown")
	}()

	errChan := make(chan error)

	// Start HTTP server.
	go func() {
		addr := fmt.Sprintf(":%d", cfg.Port)
		zap.L().Info("Starting server", zap.String("addr", addr))
		if err := echo.Start(addr); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				errChan <- errors.Wrap(err, "failed to start server")
			}
		}
	}()

	go func() {
		zap.L().Info("Starting dead letter queue consumer")
		if err := dlc.Run(); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- errors.Wrap(err, "failed to run dead letter queue consumer")
		}
	}()

	go func() {
		zap.L().Info("Starting worker consumer")
		if err := wrkc.Run(); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- errors.Wrap(err, "failed to run main queue consumer")
		}
	}()

	go func() {
		zap.L().Info("Starting completions consumer")
		if err := chc.Run(); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- errors.Wrap(err, "failed to run completions queue consumer")
		}
	}()

	api.ScheduleDeploymentOutputsCleaning(bgCtx, cfg.DeploymentsCompletedBefore, zap.L(), db)

	exit := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	// Receive output from signalChan.
	select {
	case sig := <-exit:
		zap.L().Info("Signal caught", zap.String("signal", sig.String()))
		time.Sleep(cfg.ShutdownDelay)
	case ec := <-errChan:
		zap.L().Error("Critical error received from background component", zap.Error(ec))
	}

	// drain the rest of the error channel
	go func() {
		for e := range errChan {
			zap.L().Error("Error received from background component", zap.Error(e))
		}
	}()
}
