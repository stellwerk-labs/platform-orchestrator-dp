package branchhandler

import (
	"context"
	"fmt"
	"regexp"

	"github.com/stellwerk-labs/golib/hmessaging"
	"go.uber.org/zap"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/worker/handlers"
)

type Branch struct {
	PrefixPattern *regexp.Regexp
	Handler       handlers.Handler
}

type Handler []Branch

func (h Handler) Handle(ctx context.Context, logger *zap.Logger, d hmessaging.Delivery) error {
	for _, branch := range h {
		m := branch.PrefixPattern.FindStringIndex(d.Subject)
		if m != nil && m[0] == 0 {
			return branch.Handler.Handle(ctx, logger, d)
		}
	}
	return fmt.Errorf("subject '%s' did not match any branch", d.Subject)
}
