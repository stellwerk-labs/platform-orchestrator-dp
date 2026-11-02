package api

import (
	"context"
	"time"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	"go.uber.org/zap"
)

const observationResultCleanupTimeout = 10 * time.Second

// ScheduleObservationResultCleaning uses a dedicated cadence so a result backlog
// cannot consume the ordinary output cleaner's pass or require a long write tx.
// The returned channel closes after cancellation and any current batch returns.
func ScheduleObservationResultCleaning(ctx context.Context, interval time.Duration, batchSize int, logger *zap.Logger, db model.Databaser) <-chan struct{} {
	done := make(chan struct{})
	if interval < time.Second || batchSize < 1 || batchSize > model.ObservationResultCleanupMaxBatchSize {
		logger.Error("invalid observation result cleanup bounds")
		close(done)
		return done
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				bounded, cancel := context.WithTimeout(ctx, observationResultCleanupTimeout)
				count, err := model.DeleteExpiredObservationResults(bounded, db, batchSize)
				cancel()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					logger.Error("failed to clear expired observation results", zap.Error(err))
				} else if count > 0 {
					logger.Debug("cleared expired observation result payloads", zap.Int64("count", count))
				}
			}
		}
	}()
	return done
}
