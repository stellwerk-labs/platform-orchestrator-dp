package branchhandler

import (
	"context"
	"regexp"
	"testing"

	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
)

func TestEmpty(t *testing.T) {
	h := Handler{}
	err := h.Handle(context.Background(), nil, hmessaging.Delivery{})
	require.EqualError(t, err, "subject '' did not match any branch")
}

func TestPrefixMatch(t *testing.T) {
	h := Handler{
		{PrefixPattern: regexp.MustCompile(`hello.*`), Handler: handlers.HandlerFunc(func(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error {
			return nil
		})},
	}
	require.NoError(t, h.Handle(context.Background(), nil, hmessaging.Delivery{Message: hmessaging.Message{Subject: "hello world"}}))
	err := h.Handle(context.Background(), nil, hmessaging.Delivery{Message: hmessaging.Message{Subject: "say hello"}})
	require.EqualError(t, err, "subject 'say hello' did not match any branch")
}
