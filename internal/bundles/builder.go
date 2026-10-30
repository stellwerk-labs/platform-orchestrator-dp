package bundles

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	platformorchestratorgraph "github.com/stellwerk-labs/platform-orchestrator-graph"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

const (
	ObjectStoreName = "PO_RUNNER_BUNDLES"
	// ObjectStoreTTL exceeds the command stream's 30-day maximum age, so an
	// offline runner never receives a command after its bundle has expired.
	ObjectStoreTTL       = 31 * 24 * time.Hour
	fileMode             = 0644
	directoryMode        = 0755
	ArtifactManifestName = ".stellwerk-artifacts.json"
)

func ObjectKey(orgID string, deploymentID uuid.UUID) string {
	return orgID + "/" + deploymentID.String()
}

func Build(ctx context.Context, database model.Databaser, controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface, orgID string, deploymentID uuid.UUID, executionClient ...platformorchestratorcp.ClientWithResponsesInterface) (*bytes.Buffer, error) {
	deployment, _, tofu, rawGraph, err := database.GetDeployment(ctx, nil, orgID, deploymentID, model.GetModeDefault)
	if err != nil {
		return nil, err
	}
	return buildForDeployment(ctx, controlPlaneClient, orgID, deployment, tofu, rawGraph, deployment.ModuleArtifacts, executionClient...)
}

func BuildForDeployment(
	ctx context.Context,
	controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface,
	orgID string,
	deployment *model.DeploymentSummary,
	tofu model.RawTofu,
	rawGraph model.EncodedDeploymentGraph,
	executionClient ...platformorchestratorcp.ClientWithResponsesInterface,
) (*bytes.Buffer, error) {
	return buildForDeployment(ctx, controlPlaneClient, orgID, deployment, tofu, rawGraph, deployment.ModuleArtifacts, executionClient...)
}

func buildForDeployment(
	ctx context.Context,
	controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface,
	orgID string,
	deployment *model.DeploymentSummary,
	tofu model.RawTofu,
	rawGraph model.EncodedDeploymentGraph,
	artifacts []model.ModuleArtifactRequirement,
	executionClient ...platformorchestratorcp.ClientWithResponsesInterface,
) (*bytes.Buffer, error) {
	graph, err := graphs.FromJson(bytes.NewReader(rawGraph))
	if err != nil {
		return nil, err
	}

	versionsToGet, retained, err := inlineMaterialCoordinates(graph, deployment.DeploymentEnvUuid)
	if err != nil {
		return nil, err
	}

	var inlineModules []platformorchestratorcp.InternalExecutionCatalogueModule
	if len(versionsToGet) > 0 {
		request := platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{
			AreRulesIgnored: true, PinnedModuleVersions: versionsToGet,
		}
		var requestEditors []platformorchestratorcp.RequestEditorFn
		allowNonDeployableVersions := deployment.Mode == model.DeploymentModeRollback || slices.ContainsFunc(artifacts, func(artifact model.ModuleArtifactRequirement) bool {
			return artifact.RetainedArtifactException
		})
		if allowNonDeployableVersions {
			requestEditors = append(requestEditors, func(_ context.Context, request *http.Request) error {
				request.Header.Set("X-Stellwerk-Rollback", "true")
				return nil
			})
		}
		requestEditors = append(requestEditors, func(_ context.Context, request *http.Request) error {
			request.Header.Set("X-Stellwerk-Active-Module-Versions", strings.Join(versionsToGet, ","))
			confirmations := restrictedVersionConfirmations(artifacts, versionsToGet)
			if len(confirmations) > 0 {
				request.Header.Set("X-Stellwerk-Restricted-Version-Confirmations", strings.Join(confirmations, ","))
			}
			return nil
		})
		var catalogue *platformorchestratorcp.InternalExecutionCatalogue
		if len(retained) > 0 {
			if len(executionClient) != 1 || executionClient[0] == nil {
				return nil, errors.New("runtime identity is required to reconstruct retained Draft test sources")
			}
			response, callErr := executionClient[0].GenerateInternalExecutionCatalogueWithResponse(ctx, orgID, deployment.ProjectId, deployment.EnvId, nil,
				platformorchestratorcp.InternalExecutionCatalogueGenerateBody{
					AreRulesIgnored: true, MaterialOnly: true, PinnedModuleVersions: versionsToGet, RetainedDraftTestSnapshots: retained,
				}, requestEditors...)
			if callErr != nil {
				return nil, errors.Wrap(callErr, "failed to fetch retained execution material")
			}
			if response.StatusCode() != http.StatusOK || response.JSON200 == nil {
				return nil, errors.Errorf("retained execution material unavailable (HTTP %d)", response.StatusCode())
			}
			catalogue = response.JSON200
		} else {
			response, callErr := controlPlaneClient.GenerateInternalModuleCatalogueWithResponse(ctx, orgID, deployment.ProjectId, deployment.EnvId, nil, request, requestEditors...)
			if callErr != nil {
				return nil, errors.Wrap(callErr, "failed to generate module catalogue")
			}
			if response.StatusCode() != http.StatusOK {
				return nil, errors.Errorf("unexpected status code %d when fetching module catalogue", response.StatusCode())
			}
			catalogue, err = graphs.DecodeExecutionCatalogue(response.Body, response.JSON200)
			if err != nil {
				return nil, err
			}
		}
		if err := validateRetainedDraftSources(artifacts, catalogue.Modules); err != nil {
			return nil, err
		}
		if err := validateResourceTypeContracts(artifacts, catalogue.Modules); err != nil {
			return nil, err
		}
		inlineModules, err = selectInlineModules(versionsToGet, catalogue.Modules)
		if err != nil {
			return nil, err
		}
	}

	return compressWithArtifacts(tofu, inlineModules, artifacts)
}

// Retained deployments must never acquire a different interface while their
// inline sources are reconstructed. Historical records without a contract
// identity remain readable and do not gain an invented binding.
func validateResourceTypeContracts(artifacts []model.ModuleArtifactRequirement, modules []platformorchestratorcp.InternalExecutionCatalogueModule) error {
	byCoordinate := make(map[string]platformorchestratorcp.InternalExecutionCatalogueModule, len(modules))
	for _, module := range modules {
		byCoordinate[module.Id+"@"+module.VersionId] = module
	}
	for _, artifact := range artifacts {
		if artifact.ResourceTypeVersionUUID == nil && artifact.ResourceTypeVersion == nil {
			continue
		}
		module, found := byCoordinate[artifact.ModuleID+"@"+artifact.Version]
		if !found {
			// External sources are already frozen in the retained Terraform; only
			// inline sources are requested from the catalogue for reconstruction.
			continue
		}
		if artifact.ResourceTypeVersionUUID == nil || artifact.ResourceTypeVersion == nil ||
			module.ResourceTypeVersionUuid == nil || module.ResourceTypeVersion == nil ||
			*artifact.ResourceTypeVersionUUID != *module.ResourceTypeVersionUuid ||
			*artifact.ResourceTypeVersion != *module.ResourceTypeVersion {
			return errors.Errorf("immutable Resource Type contract changed for module %s@%s", artifact.ModuleID, artifact.Version)
		}
		expected, err := json.Marshal(artifact.OutputSchema)
		if err != nil {
			return errors.Wrap(err, "failed to encode retained output interface")
		}
		actual, err := json.Marshal(module.OutputSchema)
		if err != nil {
			return errors.Wrap(err, "failed to encode selected output interface")
		}
		if !bytes.Equal(expected, actual) {
			return errors.Errorf("immutable output interface changed for module %s@%s", artifact.ModuleID, artifact.Version)
		}
	}
	return nil
}

func restrictedVersionConfirmations(artifacts []model.ModuleArtifactRequirement, versions []string) []string {
	var confirmations []string
	for _, artifact := range artifacts {
		if artifact.ConfirmedRestrictedVersionUUID != nil && *artifact.ConfirmedRestrictedVersionUUID != uuid.Nil &&
			slices.Contains(versions, artifact.ModuleID+"@"+artifact.Version) {
			confirmations = append(confirmations, artifact.ConfirmedRestrictedVersionUUID.String())
		}
	}
	slices.Sort(confirmations)
	return slices.Compact(confirmations)
}

func selectInlineModules(versions []string, modules []platformorchestratorcp.InternalExecutionCatalogueModule) ([]platformorchestratorcp.InternalExecutionCatalogueModule, error) {
	requested := make(map[string]struct{}, len(versions))
	for _, version := range versions {
		moduleID, versionID, ok := strings.Cut(version, "@")
		if !ok || moduleID == "" || versionID == "" || strings.Contains(versionID, "@") {
			return nil, errors.Errorf("invalid inline module coordinate %q", version)
		}
		requested[version] = struct{}{}
	}

	selected := make([]platformorchestratorcp.InternalExecutionCatalogueModule, 0, len(requested))
	found := make(map[string]struct{}, len(requested))
	for _, module := range modules {
		coordinate := fmt.Sprintf("%s@%s", module.Id, module.VersionId)
		if _, ok := requested[coordinate]; !ok {
			continue
		}
		if _, duplicate := found[coordinate]; duplicate {
			return nil, errors.Errorf("duplicate inline module %s in catalogue response", coordinate)
		}
		if module.ModuleSource != "inline" || module.ModuleSourceCode == nil || strings.TrimSpace(*module.ModuleSourceCode) == "" {
			return nil, errors.Errorf("inline module %s has invalid source code", coordinate)
		}
		found[coordinate] = struct{}{}
		selected = append(selected, module)
	}
	for coordinate := range requested {
		if _, ok := found[coordinate]; !ok {
			return nil, errors.Errorf("inline module %s missing from catalogue response", coordinate)
		}
	}
	return selected, nil
}

func compressWithArtifacts(content []byte, inlineModules []platformorchestratorcp.InternalExecutionCatalogueModule, artifacts []model.ModuleArtifactRequirement) (*bytes.Buffer, error) {
	buffer := new(bytes.Buffer)
	gzipWriter := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(gzipWriter)

	if err := tarWriter.WriteHeader(&tar.Header{Name: "main.tf", Size: int64(len(content)), Mode: fileMode, ModTime: time.Now()}); err != nil {
		return nil, errors.Wrap(err, "failed to write archive header")
	}
	if _, err := tarWriter.Write(content); err != nil {
		return nil, errors.Wrap(err, "failed to write archive content")
	}
	if len(artifacts) > 0 {
		manifest, err := json.Marshal(struct {
			Version   int                               `json:"version"`
			Artifacts []model.ModuleArtifactRequirement `json:"artifacts"`
		}{Version: 1, Artifacts: artifacts})
		if err != nil {
			return nil, errors.Wrap(err, "failed to encode Module artifact manifest")
		}
		if err := tarWriter.WriteHeader(&tar.Header{Name: ArtifactManifestName, Mode: fileMode, Size: int64(len(manifest)), ModTime: time.Now()}); err != nil {
			return nil, errors.Wrap(err, "failed to write Module artifact manifest header")
		}
		if _, err := tarWriter.Write(manifest); err != nil {
			return nil, errors.Wrap(err, "failed to write Module artifact manifest")
		}
	}
	for _, module := range inlineModules {
		directoryName := fmt.Sprintf("modules/%s/%s", module.Id, module.VersionId)
		if err := tarWriter.WriteHeader(&tar.Header{Name: directoryName + "/", Mode: directoryMode, ModTime: time.Now(), Typeflag: tar.TypeDir}); err != nil {
			return nil, errors.Wrapf(err, "failed to write directory header for %s", directoryName)
		}
		if module.ModuleSourceCode == nil {
			return nil, errors.Errorf("inline module %s@%s has no source code", module.Id, module.VersionId)
		}
		if err := tarWriter.WriteHeader(&tar.Header{Name: directoryName + "/main.tf", Mode: fileMode, Size: int64(len(*module.ModuleSourceCode)), ModTime: time.Now()}); err != nil {
			return nil, errors.Wrapf(err, "failed to write file header for %s/main.tf", directoryName)
		}
		if _, err := tarWriter.Write([]byte(*module.ModuleSourceCode)); err != nil {
			return nil, errors.Wrap(err, "failed to write archive content")
		}
	}
	if err := tarWriter.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to close archive writer")
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, errors.Wrap(err, "failed to close gzip writer")
	}
	return buffer, nil
}

func validateRetainedDraftSources(artifacts []model.ModuleArtifactRequirement, modules []platformorchestratorcp.InternalExecutionCatalogueModule) error {
	for _, module := range modules {
		reference, err := graphs.DraftTestReferenceFromCatalogue(module)
		if err != nil {
			return err
		}
		var expected *graphs.DraftTestReference
		for _, artifact := range artifacts {
			if artifact.ModuleID == module.Id && artifact.Version == module.VersionId {
				expected = artifact.DraftTest
				break
			}
		}
		if !reflect.DeepEqual(expected, reference) {
			return errors.Errorf("retained Draft test provenance changed for %s@%s", module.Id, module.VersionId)
		}
	}
	return nil
}

func inlineMaterialCoordinates(graph *platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig], environment uuid.UUID) ([]string, []platformorchestratorcp.ModuleDraftTestCoordinate, error) {
	retained, err := graphs.RetainedDraftTestCoordinates(graph, environment)
	if err != nil {
		return nil, nil, err
	}
	versionsToGet := make([]string, 0)
	requiredDrafts := make(map[uuid.UUID]bool)
	for coordinate := range graph.DepthFirstIterate(platformorchestratorgraph.DepthFirstIteratePreOrder) {
		node := graph.Nodes[coordinate]
		if node.ModuleConfiguration != nil && !node.ModuleConfiguration.Deleted && node.ModuleConfiguration.HasInlineSource {
			versionsToGet = append(versionsToGet, fmt.Sprintf("%s@%s", node.ModuleConfiguration.DefinitionId, node.ModuleConfiguration.VersionId))
			if node.ModuleConfiguration.DraftTest != nil {
				requiredDrafts[node.ModuleConfiguration.DraftTest.SnapshotUUID] = true
			}
		}
	}
	retained = slices.DeleteFunc(retained, func(coordinate platformorchestratorcp.ModuleDraftTestCoordinate) bool {
		return !requiredDrafts[coordinate.SnapshotUuid]
	})
	slices.Sort(versionsToGet)
	versionsToGet = slices.Compact(versionsToGet)

	return versionsToGet, retained, nil
}
