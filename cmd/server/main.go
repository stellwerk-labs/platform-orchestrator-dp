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
	"strings"
	"syscall"
	"time"

	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hconfig"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/hmessaging/reliableoutbox"
	"github.com/stellwerk-labs/golib/hnats"
	"github.com/stellwerk-labs/golib/hretrier"
	"github.com/stellwerk-labs/golib/hstandardoutbox"
	"github.com/stellwerk-labs/golib/hvaultapi"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/bundles"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/config"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/credentials/oidc"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/runners"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"

	vaultapi "github.com/hashicorp/vault/api"
	htelemetry "github.com/stellwerk-labs/golib/htelemetry"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	concurrentLogDeletes   = 20
	natsConnectTimeout     = 5 * time.Second
	runnerLogsObjectStore  = "PO_RUNNER_LOGS"
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

	natsConnection, err := hnats.Connect(hnats.ConnectionConfig{
		URLs:            []string{cfg.NATSURL},
		Name:            "platform-orchestrator-dp",
		Token:           cfg.NATSToken,
		CredentialsFile: cfg.NATSCredsFile,
		CAFile:          cfg.NATSCAFile,
		ClientCertFile:  cfg.NATSClientCertFile,
		ClientKeyFile:   cfg.NATSClientKeyFile,
		ConnectTimeout:  natsConnectTimeout,
		ReconnectWait:   time.Second,
		MaxReconnects:   -1,
	}, zap.L())
	if err != nil {
		zap.L().Fatal("failed to initialize NATS connection", zap.Error(err))
	}
	defer func() {
		if err := natsConnection.Drain(); err != nil {
			zap.L().Error("failed to drain NATS connection", zap.Error(err))
		}
		natsConnection.Close()
	}()
	js, err := hnats.NewJetStream(natsConnection)
	if err != nil {
		zap.L().Fatal("failed to initialize JetStream", zap.Error(err))
	}
	if cfg.NATSBootstrap {
		if err := hnats.EnsureStandardStreams(ctx, js, cfg.NATSStreamReplicas); err != nil {
			zap.L().Fatal("failed to bootstrap JetStream topology", zap.Error(err))
		}
	}
	runnerLogsStore, err := js.ObjectStore(ctx, runnerLogsObjectStore)
	if errors.Is(err, jetstream.ErrBucketNotFound) && cfg.NATSBootstrap {
		runnerLogsStore, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
			Bucket: runnerLogsObjectStore, Storage: jetstream.FileStorage, Replicas: cfg.NATSStreamReplicas,
		})
	}
	if err != nil {
		zap.L().Fatal("failed to bind runner logs NATS Object Store", zap.Error(err))
	}
	runnerBundlesStore, err := js.ObjectStore(ctx, bundles.ObjectStoreName)
	if errors.Is(err, jetstream.ErrBucketNotFound) && cfg.NATSBootstrap {
		runnerBundlesStore, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
			Bucket: bundles.ObjectStoreName, Storage: jetstream.FileStorage, Replicas: cfg.NATSStreamReplicas, TTL: bundles.ObjectStoreTTL,
		})
	}
	if err != nil {
		zap.L().Fatal("failed to bind runner bundles NATS Object Store", zap.Error(err))
	}
	eventPublisher := hnats.NewPublisher(js, hmessaging.EventsStreamName, zap.L())
	runnerCommandPublisher := hnats.NewPublisher(js, hmessaging.RunnerCommandsStreamName, zap.L())
	dlqPublisher := hnats.NewPublisher(js, hmessaging.DeadLettersStreamName, zap.L())
	hstandardoutbox.MessageIDPrefix = "platform-orchestrator-dp-"

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
		EventPublisher:           eventPublisher,
		Database:                 db,
		Logger:                   zap.L(),
		ControlPlaneClient:       cpClient,
		RunnerTokenSalt:          cfg.RunnerTokenSalt,
		DeploymentCompletedHooks: &completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}]{MaximumWaitersPerHandle: completionhooks.MaximumWaitersPerDeploymentHandler},
		Vault:                    vlt,
		OidcIssuerUrl:            cfg.OidcIssuerUrl,
		IamClient:                iamClient,
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
		reliableoutbox.ScheduledFlushPendingMessages(bgCtx, server.Database.AsReliableOutboxStore(), eventPublisher, reliableoutbox.DefaultScheduledFlushPeriodFunc)
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

	server.RunnerLogsReader = func(ctx context.Context, filename string) (io.ReadCloser, error) {
		reader, err := runnerLogsStore.Get(ctx, filename)
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil, api.ErrRunnerLogsNotFound
		}
		if err != nil {
			return nil, errors.Wrap(err, "failed to read runner logs from NATS Object Store")
		}
		return reader, nil
	}

	var oidcProvider oidc.Provider
	if cfg.OidcIssuerUrl != "" {
		oidcProvider = oidc.NewProvider(cfg.OidcIssuerUrl, vlt, oidc.ProviderOptions{})
	}

	wrk := &worker.Worker{
		NATSConnection:               natsConnection,
		JetStream:                    js,
		DLQPublisher:                 dlqPublisher,
		EventPublisher:               eventPublisher,
		RemoteRunnerCommandPublisher: &runners.NATSRemoteRunnerCommandPublisher{Publisher: runnerCommandPublisher},
		DB:                           db,
		Logger:                       zap.L().Named("worker"),
		ControlPlaneClient:           cpClient,
		VaultClient:                  vlt,
		RunnerImage:                  cfg.RunnerImage,
		OidcProvider:                 oidcProvider,
		HTTPClient:                   http.DefaultClient,
		RunnerCommandTTL:             cfg.RunnerCommandTTL,
		RunnerNATSConfiguration: runners.RunnerNATSConfiguration{
			URL: cfg.RunnerNATSURL, Token: cfg.RunnerNATSToken,
		},
		RunnerBundleStore: runnerBundlesStore,
		RunnerLogsDeleter: func(ctx context.Context, envUuid string) error {
			g, gctx := errgroup.WithContext(ctx)
			g.SetLimit(concurrentLogDeletes)
			objects, err := runnerLogsStore.List(gctx)
			if err != nil && !errors.Is(err, jetstream.ErrNoObjectsFound) {
				return errors.Wrap(err, "failed to list NATS runner log objects for deletion")
			}
			for _, object := range objects {
				if strings.HasPrefix(object.Name, envUuid+"/") {
					objectName := object.Name
					g.Go(func() error { return runnerLogsStore.Delete(gctx, objectName) })
				}
			}
			return g.Wait()
		},
		KubernetesRunnerPodSchedulingDelay: cfg.KubernetesRunnerPodSchedulingDelay,
		MetadataOutputKey:                  cfg.MetadataOutputKey,
	}
	wrkc, err := wrk.BuildMainConsumer(bgCtx)
	if err != nil {
		zap.L().Fatal("failed to setup main consumer", zap.Error(err))
	}
	defer func() {
		zap.L().Info("Shutting down main consumer")
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := wrkc.Close(ctx); err != nil {
			zap.L().Error("failed to close main consumer", zap.Error(err))
		}
		zap.L().Info("Main consumer shutdown")
	}()

	resultsConsumer, err := wrk.BuildRunnerResultsConsumer(bgCtx, &server)
	if err != nil {
		zap.L().Fatal("failed to setup runner results consumer", zap.Error(err))
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := resultsConsumer.Close(ctx); err != nil {
			zap.L().Error("failed to close runner results consumer", zap.Error(err))
		}
	}()

	statusConsumer, err := wrk.BuildRunnerStatusConsumer(bgCtx)
	if err != nil {
		zap.L().Fatal("failed to setup runner status consumer", zap.Error(err))
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := statusConsumer.Close(ctx); err != nil {
			zap.L().Error("failed to close runner status consumer", zap.Error(err))
		}
	}()

	completionSubscription, err := wrk.BuildCompletionsSubscription(server.DeploymentCompletedHooks)
	if err != nil {
		zap.L().Fatal("failed to setup deployment completion subscription", zap.Error(err))
	}
	defer func() {
		if err := completionSubscription.Drain(); err != nil {
			zap.L().Error("failed to drain deployment completion subscription", zap.Error(err))
		}
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
		zap.L().Info("Starting worker consumer")
		if err := wrkc.Run(bgCtx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- errors.Wrap(err, "failed to run main queue consumer")
		}
	}()

	go func() {
		zap.L().Info("Starting runner results consumer")
		if err := resultsConsumer.Run(bgCtx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- errors.Wrap(err, "failed to run runner results consumer")
		}
	}()

	go func() {
		zap.L().Info("Starting runner status consumer")
		if err := statusConsumer.Run(bgCtx); err != nil && !errors.Is(err, context.Canceled) {
			errChan <- errors.Wrap(err, "failed to run runner status consumer")
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
