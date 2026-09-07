package bundles

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
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

func Build(ctx context.Context, database model.Databaser, controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface, orgID string, deploymentID uuid.UUID) (*bytes.Buffer, error) {
	deployment, _, tofu, rawGraph, err := database.GetDeployment(ctx, nil, orgID, deploymentID, model.GetModeDefault)
	if err != nil {
		return nil, err
	}
	return buildForDeployment(ctx, controlPlaneClient, orgID, deployment, tofu, rawGraph, deployment.ModuleArtifacts)
}

func BuildForDeployment(ctx context.Context, controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface, orgID string, deployment *model.DeploymentSummary, tofu model.RawTofu, rawGraph model.EncodedDeploymentGraph) (*bytes.Buffer, error) {
	return buildForDeployment(ctx, controlPlaneClient, orgID, deployment, tofu, rawGraph, deployment.ModuleArtifacts)
}

func buildForDeployment(ctx context.Context, controlPlaneClient platformorchestratorcp.ClientWithResponsesInterface, orgID string, deployment *model.DeploymentSummary, tofu model.RawTofu, rawGraph model.EncodedDeploymentGraph, artifacts []model.ModuleArtifactRequirement) (*bytes.Buffer, error) {
	graph, err := graphs.FromJson(bytes.NewReader(rawGraph))
	if err != nil {
		return nil, err
	}

	versionsToGet := make([]string, 0)
	for coordinate := range graph.DepthFirstIterate(platformorchestratorgraph.DepthFirstIteratePreOrder) {
		node := graph.Nodes[coordinate]
		if node.ModuleConfiguration != nil && node.ModuleConfiguration.HasInlineSource {
			versionsToGet = append(versionsToGet, fmt.Sprintf("%s@%s", node.ModuleConfiguration.DefinitionId, node.ModuleConfiguration.VersionId))
		}
	}

	var inlineModules []platformorchestratorcp.InternalModuleCatalogueModule
	if len(versionsToGet) > 0 {
		request := platformorchestratorcp.GenerateInternalModuleCatalogueJSONRequestBody{
			AreRulesIgnored: true, PinnedModuleVersions: versionsToGet,
		}
		var response *platformorchestratorcp.GenerateInternalModuleCatalogueResponse
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
		response, err = controlPlaneClient.GenerateInternalModuleCatalogueWithResponse(ctx, orgID, deployment.ProjectId, deployment.EnvId, request, requestEditors...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate module catalogue")
		}
		if response.StatusCode() != http.StatusOK {
			return nil, errors.Errorf("unexpected status code %d when fetching module catalogue: %s", response.StatusCode(), string(response.Body))
		}
		inlineModules = response.JSON200.Modules
	}

	return compressWithArtifacts(tofu, inlineModules, artifacts)
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

func compressWithArtifacts(content []byte, inlineModules []platformorchestratorcp.InternalModuleCatalogueModule, artifacts []model.ModuleArtifactRequirement) (*bytes.Buffer, error) {
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
