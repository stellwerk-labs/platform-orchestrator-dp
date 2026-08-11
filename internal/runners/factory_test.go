package runners

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunnerGatewayConfigurationSelectsEndpointByRunnerLocation(t *testing.T) {
	configuration := RunnerGatewayConfiguration{
		PublicURL:       "https://api.example.test/runner-gateway",
		InternalURL:     "http://runner-gateway:8080/runner-gateway",
		RunnerTokenSalt: "salt",
	}

	assert.Equal(t, "https://api.example.test/runner-gateway", configuration.public().URL)
	assert.Equal(t, "http://runner-gateway:8080/runner-gateway", configuration.internal().URL)
	assert.Equal(t, "salt", configuration.public().RunnerTokenSalt)
}

func TestRunnerGatewayConfigurationUsesPublicEndpointWithoutInternalOverride(t *testing.T) {
	configuration := RunnerGatewayConfiguration{PublicURL: "https://api.example.test/runner-gateway"}

	assert.Equal(t, configuration.PublicURL, configuration.internal().URL)
}
