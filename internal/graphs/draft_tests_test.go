package graphs

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"
)

func draftCatalogueModule() cp.InternalExecutionCatalogueModule {
	code := `output "name" { value = "A" }`
	contract := uuid.New()
	number := int64(1)
	shape := cp.ModuleOutputSchema{}
	body := cp.ModuleVersionPublishBody{
		SemanticVersion:  "1.2.0",
		ModuleSource:     "inline",
		ModuleSourceCode: &code,
		OutputSchema:     &shape,
		ModuleInputs:     map[string]any{},
		ModuleParams:     map[string]cp.ModuleParamItem{},
		ProviderMapping:  map[string]string{},
		Dependencies:     map[string]cp.ModuleDependencyManifest{},
		Coprovisioned:    []cp.ModuleCoProvisionManifest{},
	}
	snapshot := &cp.ModuleDraftTestSnapshot{
		Id:                      uuid.New(),
		Provenance:              cp.DraftTest,
		ModuleUuid:              uuid.New(),
		ModuleSlug:              "storage",
		OrgId:                   "example",
		DraftId:                 uuid.New(),
		DraftRevision:           1,
		ProjectUuid:             uuid.New(),
		EnvironmentUuid:         uuid.New(),
		ResourceTypeVersion:     1,
		ResourceTypeVersionUuid: contract,
		Definition:              body,
	}
	return cp.InternalExecutionCatalogueModule{
		Id:                      "storage",
		OrgId:                   "example",
		ModuleUuid:              snapshot.ModuleUuid,
		VersionId:               snapshot.Id.String(),
		VersionUuid:             snapshot.Id,
		ResourceType:            "generic",
		ResourceTypeVersion:     &number,
		ResourceTypeVersionUuid: &contract,
		DraftTest:               snapshot,
		ModuleSource:            body.ModuleSource,
		ModuleSourceCode:        body.ModuleSourceCode,
		OutputSchema:            body.OutputSchema,
		ModuleInputs:            body.ModuleInputs,
		ModuleParams:            body.ModuleParams,
		ProviderMapping:         body.ProviderMapping,
		Dependencies:            body.Dependencies,
		Coprovisioned:           body.Coprovisioned,
		Rules:                   []cp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}},
	}
}

func TestDraftTestCatalogueRejectsConflictingExecutableMaterial(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*cp.InternalExecutionCatalogueModule)
	}{
		{"source substitution", func(m *cp.InternalExecutionCatalogueModule) { code := "different source"; m.ModuleSourceCode = &code }},
		{"fake lifecycle", func(m *cp.InternalExecutionCatalogueModule) {
			status := cp.ModuleVersionSemanticStatusProposed
			m.SemanticStatus = &status
		}},
		{"fake release version", func(m *cp.InternalExecutionCatalogueModule) { version := "1.2.0"; m.SemanticVersion = &version }},
		{"changed coordinate", func(m *cp.InternalExecutionCatalogueModule) { m.VersionUuid = uuid.New() }},
		{"changed contract", func(m *cp.InternalExecutionCatalogueModule) { id := uuid.New(); m.ResourceTypeVersionUuid = &id }},
		{"definition interface mismatch", func(m *cp.InternalExecutionCatalogueModule) {
			version := int64(2)
			m.DraftTest.Definition.ResourceTypeVersion = &version
		}},
		{"missing scope", func(m *cp.InternalExecutionCatalogueModule) { m.DraftTest.EnvironmentUuid = uuid.Nil }},
		{"changed inputs", func(m *cp.InternalExecutionCatalogueModule) { m.ModuleInputs = map[string]any{"secret": "different"} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			module := draftCatalogueModule()
			tt.mutate(&module)
			_, err := DraftTestReferenceFromCatalogue(module)
			require.Error(t, err)
		})
	}
}

func TestDraftTestGraphRetainsProvenanceAcrossStorageAndCopies(t *testing.T) {
	module := draftCatalogueModule()
	manifest := graph.Manifest{SharedResources: map[string]graph.ManifestResource{"store": {Type: "generic"}}}
	built, err := BuildGraph(t.Context(), manifest, []cp.InternalExecutionCatalogueModule{module}, nil)
	require.NoError(t, err)
	raw, err := ToJson(built)
	require.NoError(t, err)
	retained, err := FromJson(bytes.NewReader(raw))
	require.NoError(t, err)
	_, err = AssignModuleDefinitionsToGraphNodes(retained, []cp.InternalExecutionCatalogueModule{module})
	require.NoError(t, err)
	for _, node := range retained.Nodes {
		require.NotNil(t, node.ModuleConfiguration.DraftTest)
		require.Equal(t, module.VersionUuid, node.ModuleConfiguration.DraftTest.SnapshotUUID)
	}
	cleanup := &graph.Graph[*GraphNodeModuleConfig]{Nodes: map[graph.ResourceCoordinate]graph.ResourceNode[*GraphNodeModuleConfig]{}, Edges: map[graph.ResourceCoordinate]map[string]graph.ResourceCoordinate{}}
	AddDeletedNodes(cleanup, retained, false)
	for _, node := range cleanup.Nodes {
		require.True(t, node.ModuleConfiguration.Deleted)
		require.False(t, node.ModuleConfiguration.HasInlineSource, "deleted nodes retain provider context without fetching removed inline modules")
		require.Equal(t, module.VersionUuid, node.ModuleConfiguration.DraftTest.SnapshotUUID)
		require.Equal(t, module.ResourceTypeVersionUuid, node.ModuleConfiguration.ResourceTypeVersionUUID)
	}
	forged := module
	forged.DraftTest = nil
	_, err = AssignModuleDefinitionsToGraphNodes(retained, []cp.InternalExecutionCatalogueModule{forged})
	require.ErrorContains(t, err, "conflicting Draft test provenance")
	var document map[string]any
	require.NoError(t, json.Unmarshal(raw, &document))
	require.NotEmpty(t, document["nodes"])
}

func TestRetainedDraftCoordinatesRejectRecreatedEnvironmentAndChangedVersion(t *testing.T) {
	module := draftCatalogueModule()
	built, err := BuildGraph(t.Context(), graph.Manifest{SharedResources: map[string]graph.ManifestResource{"store": {Type: "generic"}}}, []cp.InternalExecutionCatalogueModule{module}, nil)
	require.NoError(t, err)
	retained, err := RetainedDraftTestCoordinates(built, module.DraftTest.EnvironmentUuid)
	require.NoError(t, err)
	require.Equal(t, []cp.ModuleDraftTestCoordinate{{ModuleUuid: module.ModuleUuid, SnapshotUuid: module.VersionUuid}}, retained)
	_, err = RetainedDraftTestCoordinates(built, uuid.New())
	require.Error(t, err)
	for coordinate, node := range built.Nodes {
		node.ModuleConfiguration.VersionId = uuid.NewString()
		built.Nodes[coordinate] = node
	}
	_, err = RetainedDraftTestCoordinates(built, module.DraftTest.EnvironmentUuid)
	require.Error(t, err)
}
