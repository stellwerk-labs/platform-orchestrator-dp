package api

import (
	"github.com/google/uuid"

	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
)

// Permission identifiers are kept local so the data plane can be built and
// released independently from the IAM shared module that evaluates them.
const (
	PermissionActiveResourceRead              = "active_resource_read"
	PermissionDeploymentRead                  = "deployment_read"
	PermissionDeploymentWrite                 = "deployment_write"
	PermissionDeploymentDebugRead             = "deployment_debug_read"
	PermissionMetadataKeyRead                 = "metadata_key_read"
	PermissionMetadataKeyWrite                = "metadata_key_write"
	PermissionResourceGraphRead               = "resource_graph_read"
	PermissionModuleVersionUseProposed        = "module.version.use-proposed"
	PermissionModuleVersionPinDefective       = "module.version.pin-defective"
	PermissionModuleVersionRollbackRestricted = "module.version.rollback-restricted"
)

func orgCheck(orgID, permission string) platformorchestratoriam.ResourcePermissionCheck {
	return platformorchestratoriam.ResourcePermissionCheck{
		Permission: permission,
		Resource:   "organization:" + orgID,
	}
}

func environmentCheck(environmentID uuid.UUID, permission string) platformorchestratoriam.ResourcePermissionCheck {
	return platformorchestratoriam.ResourcePermissionCheck{
		Permission: permission,
		Resource:   "env:" + environmentID.String(),
	}
}
