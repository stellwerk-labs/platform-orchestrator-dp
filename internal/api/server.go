package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"runtime/debug"
	"time"

	"github.com/labstack/echo/v4"
	echomiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/stellwerk-labs/golib/hecho"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	"github.com/stellwerk-labs/golib/hrabbitmq/reliableoutbox"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/authenticator"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/token"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/vault"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/completionhooks"
)

//go:generate go tool oapi-codegen --config=oapi-codegen.cfg.yaml --exclude-tags not-implemented ../../openapi/spec.yaml

const (
	RequestHandlerTimeout = 30 * time.Second
)

type Server struct {
	Database                   model.Databaser
	Logger                     *zap.Logger
	RabbitMqPublisher          hrabbitmq.Publisher
	ControlPlaneClient         platformorchestratorcp.ClientWithResponsesInterface
	RunnerTokenSalt            string
	DeploymentCompletedHooks   *completionhooks.CompletionHooks[completionhooks.DeploymentOrgAndId, struct{}]
	Vault                      vault.VaultClientInterface
	OidcIssuerUrl              string
	RemoteRunnerCompletedHooks *completionhooks.CompletionHooks[completionhooks.RunnerAndOrgId, completionhooks.RunnerMessage]
	IamClient                  platformorchestratoriam.ClientWithResponsesInterface
	RunnerLogsReader           RunnerLogsReader
}

func (s *Server) MapRoutes(e *echo.Echo) {
	apiHandler := NewStrictHandler(s, []StrictMiddlewareFunc{
		hecho.OperationIdCollectorMiddleware,
		hecho.BuildContextTimeoutMiddlewareWithDuration(RequestHandlerTimeout),
		middleware.NewAuthZAsserter(regexp.MustCompile("^Internal.*$")),
		hecho.AuthMiddleware(UserIdHeaderScopes),
		token.StrictEncryptionMiddleware(),
		authenticator.AuthJwtMiddleware(s.ControlPlaneClient, JwtAuthScopes, s.Logger),
	})
	RegisterHandlers(e, apiHandler)

	buildInfo, _ := debug.ReadBuildInfo()
	e.GET("/alive", func(c echo.Context) error {
		return c.String(http.StatusOK, fmt.Sprintf("%s %s", buildInfo.Main.Path, buildInfo.Main.Version))
	})
	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{
			"app":     buildInfo.Main.Path,
			"version": buildInfo.Main.Version,
			"status":  "OK",
		})
	})
	// This route is used to support integration testing to make sure we can force publish pending event messages
	// without tests having to wait 60 seconds for the next scheduled flush.
	e.POST("/internal/actions/flush-pending-messages", func(c echo.Context) error {
		if more, err := reliableoutbox.FlushNextPage(
			c.Request().Context(), zap.L(), s.Database.AsReliableOutboxStore(), 1, s.RabbitMqPublisher,
		); err != nil {
			return err
		} else {
			return c.JSON(http.StatusOK, map[string]interface{}{"more": more})
		}
	})

	// Not to break existing usage, can be removed after making sure, that there are no more requests to the old URLs
	e.Pre(echomiddleware.Rewrite(map[string]string{
		"/orgs/*/deployments/*/actions/waitForComplete":       "/orgs/$1/deployments/$2/actions/wait-for-complete",
		"/orgs/*/deployments/*/actions/getLogs":               "/orgs/$1/deployments/$2/actions/get-logs",
		"/internal/orgs/*/deployments/*/actions/forceFailure": "/internal/orgs/$1/deployments/$2/actions/force-failure",
		"/internal/orgs/*/modules/*/actions/checkUsage":       "/internal/orgs/$1/modules/$2/actions/check-usage",
	}))
}

// StrictServerInterface is the interface that your Server implementation should generate methods for.
// This line should fail if you're missing some methods. If you want to add methods to the specification, without
// implementing them, consider tagging them with the "not-implemented" tag.
var _ StrictServerInterface = (*Server)(nil)

// MustDecodeOpenApiSpec returns the value from decodeSpec via the cached value in rawSpec and panics if there was an error.
func MustDecodeOpenApiSpec() []byte {
	if b, err := rawSpec(); err != nil {
		panic(err)
	} else {
		return b
	}
}

type RunnerLogsReader func(ctx context.Context, filename string) (io.ReadCloser, error)
