package middleware

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/stellwerk-labs/golib/hlogger"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stellwerk-labs/golib/htelemetry"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/logging"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
)

func wrapWithPanicRecovery(next handlers.Handler) handlers.Handler {
	return handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error {
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
	return handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return next.Handle(ctx, logger, d)
	})
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
) handlers.HandlerFunc {
	next = wrapWithTimeout(next, time.Minute)
	next = wrapWithPanicRecovery(next)

	return handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error {
		span := htelemetry.StartSpan(
			operationName,
			htelemetry.ResourceName(d.Subject),
			htelemetry.Tag("messaging.system", "nats"),
			htelemetry.Tag("messaging.destination.name", d.Subject),
			htelemetry.Tag("messaging.message.id", d.ID),
			htelemetry.Tag("messaging.delivery.attempt", d.Attempt),
		)
		defer span.Finish()

		ctx = htelemetry.ContextWithSpan(ctx, span)

		annotateMap := make(map[string]string)
		ctx = context.WithValue(ctx, annotateSet, annotateMap)

		logger = hlogger.TraceScopedLoggerFromSpan(logger, span).With(
			zap.Object("nats", zapcore.ObjectMarshalerFunc(func(encoder zapcore.ObjectEncoder) error {
				encoder.AddString("subject", d.Subject)
				encoder.AddString("message-id", d.ID)
				encoder.AddUint64("delivery-attempt", d.Attempt)
				return nil
			})),
		)

		var wellKnown wellKnownStructure
		if err := json.Unmarshal(d.Data, &wellKnown); err == nil {
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
