package middleware

import (
	"context"
	"regexp"

	"github.com/labstack/echo/v4"
	strictecho "github.com/oapi-codegen/runtime/strictmiddleware/echo"
	"github.com/pkg/errors"
)

const authChecked = "github.com/stellwerk-labs/platform-orchestrator-iam/auth-checked"

func NewAuthZAsserter(skipOperationPattern *regexp.Regexp) strictecho.StrictEchoMiddlewareFunc {
	return func(f strictecho.StrictEchoHandlerFunc, operationID string) strictecho.StrictEchoHandlerFunc {
		// skip this middleware entirely if auth assert is disabled
		if skipOperationPattern.MatchString(operationID) {
			return f
		}

		return func(c echo.Context, request interface{}) (response interface{}, err error) {
			r, err := f(c, request)
			if i := c.Get(authChecked); i == nil && err == nil {
				return nil, errors.Errorf("all public API methods must authorize the request: %v", r)
			}
			return r, err
		}
	}
}

func SetAuthChecked(c echo.Context) {
	c.Set(authChecked, true)
}

func SetAuthCheckedCtx(ctx context.Context) {
	if i := GetEchoCtx(ctx); i != nil {
		i.Set(authChecked, true)
	}
}
