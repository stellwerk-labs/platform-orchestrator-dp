package middleware

import (
	"context"

	"github.com/labstack/echo/v4"
)

type contextKey string

var echoCtxKey contextKey = "echo-ctx"

func EchoCtxMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		r := c.Request()
		c.SetRequest(r.WithContext(context.WithValue(r.Context(), echoCtxKey, c)))
		return next(c)
	}
}

func GetEchoCtx(ctx context.Context) echo.Context {
	if i := ctx.Value(echoCtxKey); i != nil {
		if c, ok := i.(echo.Context); ok {
			return c
		}
	}
	return nil
}
