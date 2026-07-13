package branchhandler

import (
	"context"
	"regexp"
	"testing"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"github.com/wagslane/go-rabbitmq"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
)

func TestEmpty(t *testing.T) {
	h := Handler{}
	err := h.Handle(context.Background(), nil, &rabbitmq.Delivery{Delivery: amqp091.Delivery{RoutingKey: ""}})
	require.EqualError(t, err, "key '' did not match any branch")
}

func TestPrefixMatch(t *testing.T) {
	h := Handler{
		{PrefixPattern: regexp.MustCompile(`hello.*`), Handler: handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d *rabbitmq.Delivery) error {
			return nil
		})},
	}
	require.NoError(t, h.Handle(context.Background(), nil, &rabbitmq.Delivery{Delivery: amqp091.Delivery{RoutingKey: "hello world"}}))
	err := h.Handle(context.Background(), nil, &rabbitmq.Delivery{Delivery: amqp091.Delivery{RoutingKey: "say hello"}})
	require.EqualError(t, err, "key 'say hello' did not match any branch")
}
