package bundles

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestObjectContract(t *testing.T) {
	deploymentID := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	assert.Equal(t, "org-1/"+deploymentID.String(), ObjectKey("org-1", deploymentID))
	assert.Greater(t, ObjectStoreTTL, 30*24*time.Hour)
}

func TestRestrictedConfirmationIsBoundToFrozenArtifactCoordinate(t *testing.T) {
	versionUUID, unrelatedUUID, zeroUUID := uuid.New(), uuid.New(), uuid.Nil
	artifacts := []model.ModuleArtifactRequirement{
		{ModuleID: "checkout", Version: "1.0.0", ConfirmedRestrictedVersionUUID: &versionUUID},
		{ModuleID: "checkout", Version: "1.0.0", ConfirmedRestrictedVersionUUID: &versionUUID},
		{ModuleID: "checkout", Version: "2.0.0", ConfirmedRestrictedVersionUUID: &unrelatedUUID},
		{ModuleID: "unconfirmed", Version: "1.0.0"},
		{ModuleID: "zero", Version: "1.0.0", ConfirmedRestrictedVersionUUID: &zeroUUID},
	}
	assert.Equal(t, []string{versionUUID.String()}, restrictedVersionConfirmations(artifacts, []string{"checkout@1.0.0", "unconfirmed@1.0.0", "zero@1.0.0"}))
	assert.Empty(t, restrictedVersionConfirmations(artifacts, []string{"checkout@3.0.0"}))
}

func TestResourceTypeContractReconstructionRejectsSubstitution(t *testing.T) {
	id, replacement := uuid.New(), uuid.New()
	number := int64(2)
	output := map[string]any{"type": "object"}
	declared := platformorchestratorcp.ModuleOutputSchema(output)
	artifacts := []model.ModuleArtifactRequirement{{ModuleID: "database", Version: "2.0.0", ResourceTypeVersion: &number, ResourceTypeVersionUUID: &id, OutputSchema: &output}}
	modules := []platformorchestratorcp.InternalExecutionCatalogueModule{{Id: "database", VersionId: "2.0.0", ResourceTypeVersion: &number, ResourceTypeVersionUuid: &id, OutputSchema: &declared}}
	require.NoError(t, validateResourceTypeContracts(artifacts, modules))
	modules[0].ResourceTypeVersionUuid = &replacement
	require.ErrorContains(t, validateResourceTypeContracts(artifacts, modules), "immutable Resource Type contract changed")
	modules[0].ResourceTypeVersionUuid = &id
	modules[0].OutputSchema = nil
	require.ErrorContains(t, validateResourceTypeContracts(artifacts, modules), "immutable output interface changed")
	artifacts[0].ResourceTypeVersion, artifacts[0].ResourceTypeVersionUUID, artifacts[0].OutputSchema = nil, nil, nil
	require.NoError(t, validateResourceTypeContracts(artifacts, modules), "retained legacy records remain readable")
}

func TestCompressWithArtifactsIncludesRunnerVerificationManifest(t *testing.T) {
	requirements := []model.ModuleArtifactRequirement{
		{ModuleID: "namespace", Version: "1.1.0", Source: "inline", ArtifactDigest: ""},
		{ModuleID: "database", Version: "2.4.0", Source: "registry.example/database@2.4.0", ArtifactDigest: "sha256:abc"},
		{ModuleID: "legacy-database", Version: "opaque-v0", Source: "git::https://example.com/legacy", MigrationGeneration: "v0", RetainedArtifactException: true},
		{ModuleID: "first-managed", Version: "opaque-v1", SemanticVersion: "1.0.0", MigrationGeneration: "v1", Source: "inline"},
	}
	bundle, err := compressWithArtifacts([]byte("terraform {}"), nil, requirements)
	require.NoError(t, err)
	gzipReader, err := gzip.NewReader(bundle)
	require.NoError(t, err)
	tarReader := tar.NewReader(gzipReader)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			t.Fatal("artifact manifest missing from runner bundle")
		}
		require.NoError(t, err)
		if header.Name != ArtifactManifestName {
			continue
		}
		var manifest struct {
			Version   int                               `json:"version"`
			Artifacts []model.ModuleArtifactRequirement `json:"artifacts"`
		}
		require.NoError(t, json.NewDecoder(tarReader).Decode(&manifest))
		require.Equal(t, 1, manifest.Version)
		require.Equal(t, requirements, manifest.Artifacts)
		return
	}
}

func TestSelectInlineModulesIgnoresExtraPinnedCatalogueVersions(t *testing.T) {
	inlineSource := "resource \"test_resource\" \"inline\" {}"
	externalGitSource := "git::https://example.com/storefront-deployment.git"
	externalArtifact := model.ModuleArtifactRequirement{
		ModuleID:       "storefront-deployment",
		Version:        "1.3.0",
		Source:         externalGitSource,
		ArtifactDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	modules := []platformorchestratorcp.InternalExecutionCatalogueModule{
		{Id: "checkout", VersionId: "1.0.0", ModuleSource: "inline", ModuleSourceCode: &inlineSource},
		{Id: "storefront-deployment", VersionId: "1.1.0", ModuleSource: externalGitSource},
	}

	selected, err := selectInlineModules([]string{"checkout@1.0.0", "checkout@1.0.0"}, modules)
	require.NoError(t, err)
	require.Equal(t, []platformorchestratorcp.InternalExecutionCatalogueModule{modules[0]}, selected)

	bundle, err := compressWithArtifacts([]byte("terraform {}"), selected, []model.ModuleArtifactRequirement{externalArtifact})
	require.NoError(t, err)
	reader, err := gzip.NewReader(bundle)
	require.NoError(t, err)
	archive := tar.NewReader(reader)
	var files []string
	var bundledInlineSource string
	var manifest struct {
		Version   int                               `json:"version"`
		Artifacts []model.ModuleArtifactRequirement `json:"artifacts"`
	}
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		files = append(files, header.Name)
		switch header.Name {
		case "modules/checkout/1.0.0/main.tf":
			data, err := io.ReadAll(archive)
			require.NoError(t, err)
			bundledInlineSource = string(data)
		case ArtifactManifestName:
			require.NoError(t, json.NewDecoder(archive).Decode(&manifest))
		}
	}
	require.Equal(t, []string{"main.tf", ArtifactManifestName, "modules/checkout/1.0.0/", "modules/checkout/1.0.0/main.tf"}, files)
	require.Equal(t, inlineSource, bundledInlineSource)
	require.Equal(t, 1, manifest.Version)
	require.Equal(t, []model.ModuleArtifactRequirement{externalArtifact}, manifest.Artifacts)
}

func TestSelectInlineModulesRequiresEachRequestedVersionExactlyOnceWithSource(t *testing.T) {
	source := "terraform {}"
	valid := platformorchestratorcp.InternalExecutionCatalogueModule{Id: "checkout", VersionId: "1.0.0", ModuleSource: "inline", ModuleSourceCode: &source}
	for _, test := range []struct {
		name    string
		modules []platformorchestratorcp.InternalExecutionCatalogueModule
	}{
		{name: "missing", modules: nil},
		{name: "duplicate", modules: []platformorchestratorcp.InternalExecutionCatalogueModule{valid, valid}},
		{name: "missing source", modules: []platformorchestratorcp.InternalExecutionCatalogueModule{{Id: "checkout", VersionId: "1.0.0", ModuleSource: "inline"}}},
		{name: "wrong source kind", modules: []platformorchestratorcp.InternalExecutionCatalogueModule{{Id: "checkout", VersionId: "1.0.0", ModuleSource: "registry", ModuleSourceCode: &source}}},
		{name: "empty source", modules: []platformorchestratorcp.InternalExecutionCatalogueModule{{Id: "checkout", VersionId: "1.0.0", ModuleSource: "inline", ModuleSourceCode: new(string)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := selectInlineModules([]string{"checkout@1.0.0"}, test.modules)
			require.Error(t, err)
		})
	}
}

func TestRetainedDraftSourceValidationRejectsChangedOrMissingProvenance(t *testing.T) {
	source := `output "name" { value = "A" }`
	contract, number := uuid.New(), int64(1)
	definition := platformorchestratorcp.ModuleVersionPublishBody{SemanticVersion: "1.2.0", ModuleSource: "inline", ModuleSourceCode: &source}
	snapshot := &platformorchestratorcp.ModuleDraftTestSnapshot{
		Id:                      uuid.New(),
		ModuleUuid:              uuid.New(),
		DraftId:                 uuid.New(),
		DraftRevision:           1,
		ProjectUuid:             uuid.New(),
		EnvironmentUuid:         uuid.New(),
		ModuleSlug:              "storage",
		OrgId:                   "example",
		Provenance:              platformorchestratorcp.DraftTest,
		ResourceTypeVersion:     1,
		ResourceTypeVersionUuid: contract,
		Definition:              definition,
	}
	module := platformorchestratorcp.InternalExecutionCatalogueModule{
		Id:                      "storage",
		OrgId:                   "example",
		ModuleUuid:              snapshot.ModuleUuid,
		VersionUuid:             snapshot.Id,
		VersionId:               snapshot.Id.String(),
		ResourceTypeVersion:     &number,
		ResourceTypeVersionUuid: &contract,
		DraftTest:               snapshot,
		ModuleSource:            "inline",
		ModuleSourceCode:        &source,
	}
	provenance, err := graphs.DraftTestReferenceFromCatalogue(module)
	require.NoError(t, err)
	artifacts := []model.ModuleArtifactRequirement{{ModuleID: module.Id, ModuleUUID: module.ModuleUuid, Version: module.VersionId, VersionUUID: module.VersionUuid, DraftTest: provenance}}
	require.NoError(t, validateRetainedDraftSources(artifacts, []platformorchestratorcp.InternalExecutionCatalogueModule{module}))
	changed := module
	different := "changed source B"
	changed.ModuleSourceCode = &different
	require.Error(t, validateRetainedDraftSources(artifacts, []platformorchestratorcp.InternalExecutionCatalogueModule{changed}))
	missing := module
	missing.DraftTest = nil
	require.Error(t, validateRetainedDraftSources(artifacts, []platformorchestratorcp.InternalExecutionCatalogueModule{missing}))
	foreign := *provenance
	foreign.EnvironmentUUID = uuid.New()
	artifacts[0].DraftTest = &foreign
	require.Error(t, validateRetainedDraftSources(artifacts, []platformorchestratorcp.InternalExecutionCatalogueModule{module}))
}

func TestInlineMaterialRetainsOnlySourcesRequiredByLiveNodes(t *testing.T) {
	environment, project, module := uuid.New(), uuid.New(), uuid.New()
	a := &graphs.DraftTestReference{ModuleUUID: module, SnapshotUUID: uuid.New(), DraftUUID: uuid.New(), DraftRevision: 1, EnvironmentUUID: environment, ProjectUUID: project}
	b := *a
	b.SnapshotUUID = uuid.New()
	b.DraftRevision = 2
	first := graph.ResourceCoordinate{Type: "greeting", Class: "default", Id: "shared.live"}
	deleted := graph.ResourceCoordinate{Type: "greeting", Class: "default", Id: "shared.removed"}
	stored := &graph.Graph[*graphs.GraphNodeModuleConfig]{Nodes: map[graph.ResourceCoordinate]graph.ResourceNode[*graphs.GraphNodeModuleConfig]{
		first:   {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "greeting", VersionId: a.SnapshotUUID.String(), DraftTest: a, HasInlineSource: true}},
		deleted: {ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "greeting", VersionId: b.SnapshotUUID.String(), DraftTest: &b, Deleted: true, HasInlineSource: false}},
	}, Edges: map[graph.ResourceCoordinate]map[string]graph.ResourceCoordinate{}}
	versions, retained, err := inlineMaterialCoordinates(stored, environment)
	require.NoError(t, err)
	require.Equal(t, []string{"greeting@" + a.SnapshotUUID.String()}, versions)
	require.Equal(t, []platformorchestratorcp.ModuleDraftTestCoordinate{{ModuleUuid: module, SnapshotUuid: a.SnapshotUUID}}, retained, "deleted provider context must not be reconstructed as a live executable")
	b.EnvironmentUUID = uuid.New()
	_, _, err = inlineMaterialCoordinates(stored, environment)
	require.Error(t, err, "even deleted references may not borrow another Environment's provenance")
}
