package api

import (
	"encoding/json"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
)

func TestOpenAPISpecValidatesWithExamples(t *testing.T) {
	source, err := openapi3.NewLoader().LoadFromFile("../../openapi/spec.yaml")
	require.NoError(t, err)
	require.NotNil(t, source.Info)
	require.NotEmpty(t, source.Info.Title)
	require.NotEmpty(t, source.Info.Version)
	require.NoError(t, source.Validate(t.Context(), openapi3.EnableExamplesValidation()))

	generated, err := GetSwagger()
	require.NoError(t, err)
	require.Equal(t, source.Info, generated.Info)
	require.NoError(t, generated.Validate(t.Context(), openapi3.EnableExamplesValidation()))
}

func TestEventsSpecValidatesWithExamples(t *testing.T) {
	source, err := openapi3.NewLoader().LoadFromFile("../../openapi/events.yaml")
	require.NoError(t, err)
	require.NotNil(t, source.Info)
	require.NotEmpty(t, source.Info.Title)
	require.NotEmpty(t, source.Info.Version)
	require.NoError(t, source.Validate(t.Context(), openapi3.EnableExamplesValidation()))
}

func TestOptionalObservationSpecValidatesAndMatchesRunnerResultSchema(t *testing.T) {
	loader := openapi3.NewLoader()
	optional, err := loader.LoadFromFile("../../openapi/observation.yaml")
	require.NoError(t, err)
	require.NoError(t, optional.Validate(t.Context(), openapi3.EnableExamplesValidation()))
	mandatory, err := loader.LoadFromFile("../../openapi/spec.yaml")
	require.NoError(t, err)
	for _, name := range []string{"ObservationChange", "ObservationResult"} {
		expected, err := json.Marshal(mandatory.Components.Schemas[name])
		require.NoError(t, err)
		actual, err := json.Marshal(optional.Components.Schemas[name])
		require.NoError(t, err)
		require.JSONEq(t, string(expected), string(actual), "Runner transport and optional safe-result schema must be exactly equal")
	}
}
