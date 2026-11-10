package graphs

import (
	"fmt"

	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
)

// UserBadRequestError represents an error that we want to surface to the user as an HTTP-400.
type UserBadRequestError string

func (e UserBadRequestError) Error() string {
	return string(e)
}

func validateModuleDefinition(coordinate graph.ResourceCoordinate, configuration *GraphNodeModuleConfig) error {
	if configuration == nil {
		return UserBadRequestError(fmt.Sprintf("resource '%s' has no module configuration", coordinate))
	}
	if configuration.Definition == nil {
		return UserBadRequestError(fmt.Sprintf("resource '%s' requires unavailable module %s@%s; restore that exact module version and its providers before retrying",
			coordinate, configuration.DefinitionId, configuration.VersionId))
	}
	return nil
}
