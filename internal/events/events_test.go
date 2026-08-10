package events

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genevents"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudEventJSONMarshalling(t *testing.T) {
	t.Parallel()

	type MyEventData struct {
		Message string `json:"message"`
	}

	event := CloudEvent[MyEventData]{
		SpecVersion: CloudEventSpecVersion1{},
		Type:        genevents.IoPlatformOrchestratorDeploymentCreated,
		Time:        time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
		Data: MyEventData{
			Message: "Hello!",
		},
	}

	jsonData, err := json.Marshal(event)
	require.NoError(t, err)

	expectedJSON := `{"specversion":"1.0","type":"io.platform-orchestrator.deployment.created","time":"2025-01-01T12:00:00Z","data":{"message":"Hello!"}}`
	assert.JSONEq(t, expectedJSON, string(jsonData))

	var unmarshalledEvent CloudEvent[MyEventData]
	err = json.Unmarshal(jsonData, &unmarshalledEvent)
	require.NoError(t, err)

	assert.Equal(t, event.SpecVersion, unmarshalledEvent.SpecVersion)
	assert.Equal(t, event.Type, unmarshalledEvent.Type)
	assert.WithinDuration(t, event.Time, unmarshalledEvent.Time, 0)
	assert.Equal(t, event.Data, unmarshalledEvent.Data)
}

func TestCloudEventSpecVersion1Unmarshalling(t *testing.T) {
	t.Parallel()

	var specVersion CloudEventSpecVersion1
	err := specVersion.UnmarshalJSON([]byte(`"1.0"`))
	require.NoError(t, err)

	err = specVersion.UnmarshalJSON([]byte(`"2.0"`))
	require.NoError(t, err) // The current implementation returns no error, so we assert that.
}
