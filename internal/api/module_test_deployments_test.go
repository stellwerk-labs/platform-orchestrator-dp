package api

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestModuleTestSelectionRejectsAmbiguousCoordinates(t *testing.T) {
	module, snapshot := uuid.New(), uuid.New()
	require.NoError(t, validateDraftTestSelection(map[string]uuid.UUID{module.String(): snapshot}))
	for _, invalid := range []map[string]uuid.UUID{nil, {}, {"storage": snapshot}, {uuid.Nil.String(): snapshot}, {module.String(): uuid.Nil}, {"urn:uuid:" + module.String(): snapshot}} {
		require.Error(t, validateDraftTestSelection(invalid))
	}
}

func TestModuleTestQueueIntentAndReplayIdentityRetainExactSnapshots(t *testing.T) {
	module, first, second := uuid.New(), uuid.New(), uuid.New()
	original := deploymentRequest{DraftTestSnapshots: map[string]uuid.UUID{module.String(): first}}
	encoded, err := json.Marshal(original)
	require.NoError(t, err)
	var restored deploymentRequest
	require.NoError(t, json.Unmarshal(encoded, &restored))
	require.Equal(t, original.DraftTestSnapshots, restored.DraftTestSnapshots)
	require.Equal(t, moduleVersionRequestDigest(nil, nil), moduleVersionRequestDigest(nil, nil, nil))
	require.NotEqual(t, moduleVersionRequestDigest(nil, nil), moduleVersionRequestDigest(nil, nil, original.DraftTestSnapshots))
	require.NotEqual(t, moduleVersionRequestDigest(nil, nil, original.DraftTestSnapshots), moduleVersionRequestDigest(nil, nil, map[string]uuid.UUID{module.String(): second}))
}

func TestModuleTestAdmissionTargetsCannotLoseTheirProvenance(t *testing.T) {
	project, environment, module, snapshot, published := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	reference := &graphs.DraftTestReference{ModuleUUID: module, SnapshotUUID: snapshot, DraftUUID: uuid.New(), DraftRevision: 3, ProjectUUID: project, EnvironmentUUID: environment}
	testArtifact := model.ModuleArtifactRequirement{ModuleUUID: module, VersionUUID: snapshot, DraftTest: reference}
	publishedArtifact := model.ModuleArtifactRequirement{ModuleUUID: uuid.New(), VersionUUID: published}
	attempt := &OrdinaryDeploymentAdmissionAttempt{ProjectUUID: project, EnvironmentUUID: environment, Mode: model.DeploymentModeDeploy}
	require.NoError(t, attempt.collectModuleTargets([]model.ModuleArtifactRequirement{testArtifact, publishedArtifact}))
	require.Equal(t, map[string]uuid.UUID{module.String(): snapshot}, attempt.draftTestSnapshots)
	require.Len(t, attempt.moduleVersions, 2)
	receipt := attempt.receipt("pending", uuid.Nil, "")
	body := deploymentAdmissionRequest(&receipt)
	require.NotNil(t, body.DraftTestSnapshots)
	require.Equal(t, attempt.draftTestSnapshots, *body.DraftTestSnapshots)
	for _, change := range []func(*model.ModuleArtifactRequirement){
		func(a *model.ModuleArtifactRequirement) {
			r := *a.DraftTest
			r.EnvironmentUUID = uuid.New()
			a.DraftTest = &r
		},
		func(a *model.ModuleArtifactRequirement) {
			r := *a.DraftTest
			r.ProjectUUID = uuid.New()
			a.DraftTest = &r
		},
		func(a *model.ModuleArtifactRequirement) { a.VersionUUID = uuid.New() },
	} {
		invalid := testArtifact
		change(&invalid)
		require.Error(t, attempt.collectModuleTargets([]model.ModuleArtifactRequirement{invalid}))
	}
	relabeled := testArtifact
	relabeled.DraftTest = nil
	require.Error(t, attempt.collectModuleTargets([]model.ModuleArtifactRequirement{testArtifact, relabeled}))
	require.Error(t, attempt.collectModuleTargets([]model.ModuleArtifactRequirement{relabeled, testArtifact}))
	attempt.Mode = model.DeploymentModeDestroy
	require.NoError(t, attempt.collectModuleTargets([]model.ModuleArtifactRequirement{testArtifact}))
	require.Empty(t, attempt.moduleVersions, "destroy keeps its empty lifecycle-evidence exception")
	require.Empty(t, attempt.draftTestSnapshots)
	receipt = attempt.receipt("pending", uuid.Nil, "")
	require.Nil(t, deploymentAdmissionRequest(&receipt).DraftTestSnapshots)
}

func testExecutableCatalogue() cp.InternalExecutionCatalogueModule {
	source := "output \"name\" { value = \"A\" }"
	contract, number := uuid.New(), int64(1)
	definition := cp.ModuleVersionPublishBody{SemanticVersion: "1.2.0", ModuleSource: "inline", ModuleSourceCode: &source}
	snapshot := &cp.ModuleDraftTestSnapshot{
		Id:                      uuid.New(),
		ModuleUuid:              uuid.New(),
		DraftId:                 uuid.New(),
		DraftRevision:           1,
		ProjectUuid:             uuid.New(),
		EnvironmentUuid:         uuid.New(),
		ModuleSlug:              "storage",
		OrgId:                   "example",
		Provenance:              cp.DraftTest,
		ResourceTypeVersion:     1,
		ResourceTypeVersionUuid: contract,
		Definition:              definition,
	}
	return cp.InternalExecutionCatalogueModule{
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
}

func TestModuleTestCatalogueSelectionRejectsUnrequestedOrSubstitutedMaterial(t *testing.T) {
	module := testExecutableCatalogue()
	selection := map[string]uuid.UUID{module.ModuleUuid.String(): module.VersionUuid}
	catalogue := &cp.InternalExecutionCatalogue{Modules: []cp.InternalExecutionCatalogueModule{module}}
	resolved, err := validateCatalogueDraftSelection(catalogue, selection, nil, module.DraftTest.EnvironmentUuid, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"storage": module.VersionId}, resolved)
	_, err = validateCatalogueDraftSelection(catalogue, nil, nil, module.DraftTest.EnvironmentUuid, nil)
	require.Error(t, err)
	_, err = validateCatalogueDraftSelection(catalogue, selection, nil, uuid.New(), nil)
	require.Error(t, err)
	_, err = validateCatalogueDraftSelection(catalogue, selection, nil, module.DraftTest.EnvironmentUuid, map[string]string{"storage": "1.0.0"})
	require.Error(t, err)
	retained := []cp.ModuleDraftTestCoordinate{{ModuleUuid: module.ModuleUuid, SnapshotUuid: module.VersionUuid}}
	_, err = validateCatalogueDraftSelection(catalogue, nil, retained, module.DraftTest.EnvironmentUuid, nil)
	require.NoError(t, err)
	catalogue.Modules[0].Rules = []cp.InternalModuleCatalogueModuleRule{{ResourceClass: "default"}}
	_, err = validateCatalogueDraftSelection(catalogue, nil, retained, module.DraftTest.EnvironmentUuid, nil)
	require.Error(t, err, "retained A must not receive new B's rules")
	catalogue.Modules = append(catalogue.Modules, module)
	_, err = validateCatalogueDraftSelection(catalogue, selection, nil, module.DraftTest.EnvironmentUuid, nil)
	require.Error(t, err, "duplicate executable coordinates are ambiguous")
}
