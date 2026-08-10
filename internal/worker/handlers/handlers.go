package handlers

import (
	"context"

	"github.com/stellwerk-labs/golib/hmessaging"
	"go.uber.org/zap"
)

// A Handler is a thing which can handle a message and return a possible error if necessary. Handlers can be chained
// together to create more complex request processing chains or wrapped in middleware to provide meta behaviors.
type Handler interface {
	Handle(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error
}

type HandlerFunc func(context.Context, *zap.Logger, hmessaging.Delivery) error

func (f HandlerFunc) Handle(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error {
	return f(ctx, logger, d)
}

// A Middleware is a thing that can wrap a Handler to produce another Handler.
type Middleware interface {
	Wrap(handler Handler) Handler
}

// A MiddlewareFunc allows a function to act as a Middleware
type MiddlewareFunc func(handler Handler) Handler

func (f MiddlewareFunc) Wrap(handler Handler) Handler {
	return f(handler)
}
