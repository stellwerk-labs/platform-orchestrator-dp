package handlers

import (
	"context"

	delayqueues "github.com/stellwerk-labs/golib/hrabbitmq/delayqueues/v2"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/zap"
)

// A Handler is a thing which can handle a message and return a possible error if necessary. Handlers can be chained
// together to create more complex request processing chains or wrapped in middleware to provide meta behaviors.
type Handler interface {
	Handle(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error
}

type HandlerFunc delayqueues.HandlerFunc

func (f HandlerFunc) Handle(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error {
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
