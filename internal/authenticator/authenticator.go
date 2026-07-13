package authenticator

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v4"
	"github.com/labstack/echo/v4"
	strictecho "github.com/oapi-codegen/runtime/strictmiddleware/echo"
	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/api/middleware"
	usererrors "github.com/stellwerk-labs/platform-orchestrator-dp/internal/errors"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
)

const jwtAuthorizationScheme = "JWT"

func AuthJwtMiddleware(controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface, jwtHeaderScope string, logger *zap.Logger) strictecho.StrictEchoMiddlewareFunc {
	return func(f strictecho.StrictEchoHandlerFunc, operationID string) strictecho.StrictEchoHandlerFunc {
		return func(ctx echo.Context, args interface{}) (interface{}, error) {
			if ctx.Get(jwtHeaderScope) != nil {
				jwt := ctx.Request().Header.Get("Authorization")
				if jwt == "" {
					return nil, echo.NewHTTPError(http.StatusUnauthorized, "missing Authorization header")
				}
				authHeaderParts := strings.Split(jwt, " ")
				if len(authHeaderParts) == 2 && (authHeaderParts[0] == jwtAuthorizationScheme || authHeaderParts[0] == "Bearer") {
					jwt = authHeaderParts[1]
				} else {
					return nil, echo.NewHTTPError(http.StatusUnauthorized, errors.New("invalid Authorization header").Error())
				}

				if err := validateJwtToken(ctx.Request().Context(), controlPlaneClient, ctx.Param("orgId"), ctx.Param("runnerId"), jwt); err != nil {
					if ue := new(usererrors.UserError); errors.As(err, &ue) {
						return nil, echo.NewHTTPError(http.StatusUnauthorized, errors.Wrap(ue, "failed to validate JWT token").Error())
					} else {
						logger.Sugar().Warnw("failed to validate JWT token", logging.ZapOrgId(ctx.Param("orgId")), logging.ZapRunnerId(ctx.Param("runnerId")), zap.Error(err))
						return nil, echo.NewHTTPError(http.StatusUnauthorized, errors.New("failed to validate JWT token").Error())
					}
				}
				middleware.SetAuthChecked(ctx)
			}

			return f(ctx, args)
		}
	}
}

func validateJwtToken(ctx context.Context, cpClient platformorchestratorcp.ClientWithResponsesInterface, orgId, runnerId, jwtHeader string) error {
	var publicKeyPEM string
	if res, err := cpClient.GetRunnerWithResponse(ctx, orgId, runnerId); err != nil {
		return errors.Wrap(err, "failed to fetch runner")
	} else if res.StatusCode() == http.StatusNotFound {
		return usererrors.NewUserError(res.JSON404.Message)
	} else if res.StatusCode() != http.StatusOK {
		return errors.Errorf("unexpected status code when verifying runner: %d", res.StatusCode())
	} else {
		runnerCfg := res.JSON200.RunnerConfiguration
		runnerType, _ := runnerCfg.Discriminator()
		if runnerType != string(platformorchestratorcp.RunnerTypeKubernetesAgent) {
			return usererrors.NewUserError(fmt.Sprintf("unexpected runner type %s expected runner type is %s", runnerType, platformorchestratorcp.RunnerTypeKubernetesAgent))
		}
		k8sAgentRunnerCfg, _ := runnerCfg.AsK8sAgentRunnerConfiguration()
		publicKeyPEM = k8sAgentRunnerCfg.Key
	}

	token, err := jwt.Parse(jwtHeader, func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodEdDSA {
			return nil, errors.Errorf("unexpected signing method %s", token.Method)
		}
		block, _ := pem.Decode([]byte(publicKeyPEM))
		if block == nil || block.Type != "PUBLIC KEY" {
			return nil, errors.New("failed to decode PEM block containing public key")
		}

		publicKeyInterface, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse public key from PEM")
		}

		publicKey, ok := publicKeyInterface.(ed25519.PublicKey)
		if !ok {
			return nil, errors.New("public key is not of type ed25519.PublicKey")
		} else {
			return publicKey, nil
		}
	})
	if err != nil || !token.Valid {
		return errors.Wrap(err, "failed to validate JWT token")
	} else {
		return nil
	}
}
