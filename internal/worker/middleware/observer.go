package middleware

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/golib/hrabbitmq"
	delayqueues "github.com/stellwerk-labs/golib/hrabbitmq/delayqueues/v2"
	"github.com/stellwerk-labs/golib/htelemetry"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
)

func wrapWithPanicRecovery(next handlers.Handler) handlers.Handler {
	return handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error {
		var err error
		defer func() {
			if r := recover(); r != nil {
				err = errors.Errorf("panic: %v", r)
			}
		}()
		err = next.Handle(ctx, logger, d)
		return err
	})
}

func wrapWithTimeout(next handlers.Handler, timeout time.Duration) handlers.Handler {
	return handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return next.Handle(ctx, logger, d)
	})
}

func wrapWithGracefulRetry(next handlers.Handler, retryPublisher hrabbitmq.Publisher, cache *expirable.LRU[string, int32]) handlers.Handler {
	return handlers.HandlerFunc(delayqueues.WrapRepublishGracefulRetriesWithDelay(retryPublisher, cache, func(ctx context.Context, logger *zap.Logger, delivery *rabbitmq.Delivery) error {
		return next.Handle(ctx, logger, delivery)
	}))
}

type contextKey string

const annotateSet contextKey = "observer-annotate"

func SetObserverAnnotation(ctx context.Context, key string, value string) {
	if annotateMap, ok := ctx.Value(annotateSet).(map[string]string); ok {
		annotateMap[key] = value
	}
}

type wellKnownData struct {
	OrgId string `json:"org_id"`
}

type wellKnownStructure struct {
	Data wellKnownData `json:"data"`
}

func WrapWithObserver(
	next handlers.Handler,
	operationName string,
	retryPublisher hrabbitmq.Publisher,
	cache *expirable.LRU[string, int32],
) handlers.HandlerFunc {
	next = wrapWithGracefulRetry(next, retryPublisher, cache)
	next = wrapWithTimeout(next, time.Minute)
	next = wrapWithPanicRecovery(next)

	return handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error {
		span := htelemetry.StartSpan(
			operationName,
			append(
				hrabbitmq.ExtractSpanOptionsFromMessage(logger, d.Headers),
				htelemetry.ResourceName(d.RoutingKey),
				htelemetry.Tag("rabbitmq.routing-key", d.RoutingKey),
				htelemetry.Tag("rabbitmq.message-id", d.MessageId),
			)...,
		)
		defer span.Finish()

		ctx = htelemetry.ContextWithSpan(ctx, span)

		annotateMap := make(map[string]string)
		ctx = context.WithValue(ctx, annotateSet, annotateMap)

		logger = hlogger.TraceScopedLoggerFromSpan(logger, span).With(
			zap.Object("rabbitmq", zapcore.ObjectMarshalerFunc(func(encoder zapcore.ObjectEncoder) error {
				encoder.AddString("routing-key", d.RoutingKey)
				encoder.AddString("message-id", d.MessageId)
				return nil
			})),
		)

		var wellKnown wellKnownStructure
		if err := json.Unmarshal(d.Body, &wellKnown); err == nil {
			if wellKnown.Data.OrgId != "" {
				logger = logger.With(logging.ZapOrgId(wellKnown.Data.OrgId))
				span.SetTag("org_id", wellKnown.Data.OrgId)
			}
		}

		err := next.Handle(ctx, logger, d)
		for k, v := range annotateMap {
			logger = logger.With(zap.String(k, v))
			span.SetTag(k, v)
		}
		if err != nil {
			logger.Error("error handling message", zap.Error(err))
			span.Finish(htelemetry.WithError(err))
			return err
		}
		logger.Info("handled message")
		return nil
	})
}
