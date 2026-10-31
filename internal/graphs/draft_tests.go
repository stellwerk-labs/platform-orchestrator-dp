package graphs

import (
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
)

// DraftTestReference retains executable provenance independently of moving
// authoring heads and published Version lifecycle metadata.
type DraftTestReference struct {
	SnapshotUUID    uuid.UUID `json:"snapshot_uuid"`
	ModuleUUID      uuid.UUID `json:"module_uuid"`
	DraftUUID       uuid.UUID `json:"draft_uuid"`
	DraftRevision   int64     `json:"draft_revision"`
	ProjectUUID     uuid.UUID `json:"project_uuid"`
	EnvironmentUUID uuid.UUID `json:"environment_uuid"`
}

func (r DraftTestReference) Valid() bool {
	return r.SnapshotUUID != uuid.Nil && r.ModuleUUID != uuid.Nil && r.DraftUUID != uuid.Nil && r.DraftRevision > 0 && r.ProjectUUID != uuid.Nil && r.EnvironmentUUID != uuid.Nil
}

func DraftTestReferenceFromCatalogue(module cp.InternalExecutionCatalogueModule) (*DraftTestReference, error) {
	snapshot := module.DraftTest
	if snapshot == nil {
		return nil, nil
	}
	result := &DraftTestReference{SnapshotUUID: snapshot.Id, ModuleUUID: snapshot.ModuleUuid, DraftUUID: snapshot.DraftId, DraftRevision: snapshot.DraftRevision, ProjectUUID: snapshot.ProjectUuid, EnvironmentUUID: snapshot.EnvironmentUuid}
	if !result.Valid() || snapshot.Provenance != cp.DraftTest || snapshot.Id != module.VersionUuid || snapshot.Id.String() != module.VersionId || snapshot.ModuleUuid != module.ModuleUuid || snapshot.ModuleSlug != module.Id || snapshot.OrgId != module.OrgId {
		return nil, fmt.Errorf("invalid immutable Draft test identity for module %s", module.Id)
	}
	if module.SemanticStatus != nil || module.SemanticVersion != nil || module.MigrationGeneration != nil || module.VerificationStatus != nil {
		return nil, fmt.Errorf("draft test module %s cannot carry published Version metadata", module.Id)
	}
	if snapshot.ResourceTypeVersion < 1 || snapshot.ResourceTypeVersionUuid == uuid.Nil || module.ResourceTypeVersion == nil || module.ResourceTypeVersionUuid == nil || snapshot.ResourceTypeVersion != *module.ResourceTypeVersion ||
		snapshot.ResourceTypeVersionUuid != *module.ResourceTypeVersionUuid {
		return nil, fmt.Errorf("draft test module %s has conflicting Resource Type metadata", module.Id)
	}
	body := snapshot.Definition
	selectedInterface := int64(1)
	if body.ResourceTypeVersion != nil {
		selectedInterface = *body.ResourceTypeVersion
	}
	if selectedInterface != snapshot.ResourceTypeVersion {
		return nil, fmt.Errorf("draft test module %s has conflicting selected interface", module.Id)
	}

	if body.ModuleSource != module.ModuleSource || !reflect.DeepEqual(body.ModuleSourceCode, module.ModuleSourceCode) || !reflect.DeepEqual(body.ModuleInputs, module.ModuleInputs) || !reflect.DeepEqual(body.Dependencies, module.Dependencies) ||
		!reflect.DeepEqual(body.Coprovisioned, module.Coprovisioned) ||
		!reflect.DeepEqual(body.ProviderMapping, module.ProviderMapping) ||
		!reflect.DeepEqual(body.OutputSchema, module.OutputSchema) {
		return nil, fmt.Errorf("draft test module %s does not match its immutable definition", module.Id)
	}
	digest := ""
	if body.ArtifactDigest != nil {
		digest = *body.ArtifactDigest
	}
	if digest != module.ArtifactDigest || len(body.ModuleParams) != len(module.ModuleParams) {
		return nil, fmt.Errorf("draft test module %s has conflicting artifact or parameter metadata", module.Id)
	}
	for key, parameter := range body.ModuleParams {
		actual, found := module.ModuleParams[key]
		if !found || parameter.Type != actual.Type || parameter.IsOptional != actual.IsOptional {
			return nil, fmt.Errorf("draft test module %s has conflicting parameter declarations", module.Id)
		}
	}
	return result, nil
}

// RetainedDraftTestCoordinates identifies immutable test material needed to reconstruct a stored graph.
func RetainedDraftTestCoordinates(previous *graph.Graph[*GraphNodeModuleConfig], environment uuid.UUID) ([]cp.ModuleDraftTestCoordinate, error) {
	result := []cp.ModuleDraftTestCoordinate{}
	seen := map[string]bool{}
	if previous == nil {
		return result, nil
	}
	for _, node := range previous.Nodes {
		configuration := node.ModuleConfiguration
		if configuration == nil || configuration.DraftTest == nil {
			continue
		}
		reference := configuration.DraftTest
		if !reference.Valid() || reference.EnvironmentUUID != environment || reference.SnapshotUUID.String() != configuration.VersionId {
			return nil, fmt.Errorf("retained graph has invalid scoped Draft test provenance")
		}
		key := reference.ModuleUUID.String() + "/" + reference.SnapshotUUID.String()
		if !seen[key] {
			seen[key] = true
			result = append(result, cp.ModuleDraftTestCoordinate{ModuleUuid: reference.ModuleUUID, SnapshotUuid: reference.SnapshotUUID})
		}
	}
	slices.SortFunc(result, func(a, b cp.ModuleDraftTestCoordinate) int {
		return strings.Compare(a.ModuleUuid.String()+a.SnapshotUuid.String(), b.ModuleUuid.String()+b.SnapshotUuid.String())
	})
	return result, nil
}
