package api

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	platformorchestratorgraph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/graphs"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func TestProjectPlanCaptureDropsAllManifestAndGraphValues(t *testing.T) {
	secret := "should-never-cross-the-boundary"
	manifest, err := json.Marshal(DeploymentManifest{
		Workloads: map[string]DeploymentManifestWorkload{
			"api": {Outputs: map[string]string{"password": secret}, Resources: map[string]DeploymentManifestResource{
				"db": {Type: "postgres", Params: map[string]any{"credential": secret}},
			}},
		},
		Shared: map[string]DeploymentManifestResource{"shared": {Type: "bucket", Params: map[string]any{"token": secret}}},
	})
	require.NoError(t, err)
	graph, err := graphs.ToJson(graphWithSecretParam(t, secret))
	require.NoError(t, err)
	diff, err := json.Marshal(DeploymentDiff{Changes: []DeploymentDiffChange{}, NumAdded: 0, NumChanged: 0, NumRemoved: 0})
	require.NoError(t, err)

	projectedManifest, projectedGraph, _, err := projectPlanCapture(DeploymentPlanCapture{Manifest: manifest, Graph: graph, Diff: diff})
	require.NoError(t, err)
	encoded, err := json.Marshal(struct {
		Manifest externalManifestStructure `json:"manifest"`
		Graph    externalGraphStructure    `json:"graph"`
	}{projectedManifest, projectedGraph})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), secret)
	require.Contains(t, string(encoded), "credential")
	require.Contains(t, string(encoded), "password")
}

func TestProjectModuleInventoryIsBoundedAndOmitsPrivateMaterial(t *testing.T) {
	moduleID, versionID := uuid.New(), uuid.New()
	artifacts := []model.ModuleArtifactRequirement{{
		ModuleID: "postgres", ModuleUUID: moduleID, Version: "legacy-release", VersionUUID: versionID,
		Source: "private-source", ConfirmedRestrictedVersionUUID: &versionID,
	}}
	require.NoError(t, validateExternalStatusModuleArtifacts(artifacts))
	projected := projectModuleInventory(artifacts)
	encoded, err := json.Marshal(projected)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "private-source")
	require.NotContains(t, string(encoded), "confirmed")
	require.Equal(t, "legacy-release", projected[0].Version)
	require.Equal(t, moduleID, *projected[0].ModuleUUID)
	require.Equal(t, versionID, *projected[0].VersionUUID)

	require.ErrorContains(t, validateExternalStatusModuleArtifacts(make([]model.ModuleArtifactRequirement, maxExternalStatusModules+1)), "external status limit")
}

func graphWithSecretParam(t *testing.T, secret string) *platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig] {
	t.Helper()
	coordinate := platformorchestratorgraph.ResourceCoordinate{Type: "postgres", Class: "default", Id: "main"}
	return &platformorchestratorgraph.Graph[*graphs.GraphNodeModuleConfig]{
		Nodes: map[platformorchestratorgraph.ResourceCoordinate]platformorchestratorgraph.ResourceNode[*graphs.GraphNodeModuleConfig]{
			coordinate: {
				ModuleConfiguration: &graphs.GraphNodeModuleConfig{DefinitionId: "postgres", VersionId: "v2"},
				Params:              map[string]any{"credential": secret},
			},
		},
		Edges:           map[platformorchestratorgraph.ResourceCoordinate]map[string]platformorchestratorgraph.ResourceCoordinate{},
		Workloads:       map[string]platformorchestratorgraph.ResourceCoordinate{"api": coordinate},
		SharedResources: map[string]platformorchestratorgraph.ResourceCoordinate{},
		LeafResources:   map[platformorchestratorgraph.ResourceCoordinate]bool{coordinate: true},
	}
}
