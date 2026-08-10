package worker

import "time"

const (
	MainConsumerConcurrency   = 10
	MainConsumerMaxDeliveries = 10
	MainConsumerAckWait       = time.Minute
	RunnerStatusConcurrency   = 10
)
