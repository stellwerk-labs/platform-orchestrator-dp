package worker

import "time"

const (
	MainConsumerConcurrency = 10
	MainConsumerMessageTtl  = time.Minute * 10
)
