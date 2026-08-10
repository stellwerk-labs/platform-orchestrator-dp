package events

import (
	"time"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
)

type CloudEvent[e any] struct {
	SpecVersion CloudEventSpecVersion1 `json:"specversion"`
	Type        genevents.EventType    `json:"type"`
	Time        time.Time              `json:"time"`
	Data        e                      `json:"data"`
}

type CloudEventSpecVersion1 struct{}

func (c CloudEventSpecVersion1) MarshalJSON() ([]byte, error) {
	return []byte(`"1.0"`), nil
}

func (c CloudEventSpecVersion1) UnmarshalJSON(data []byte) error {
	return nil
}
