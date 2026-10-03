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

	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"

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
	modules := []platformorchestratorcp.InternalModuleCatalogueModule{
		{Id: "checkout", VersionId: "1.0.0", ModuleSource: "inline", ModuleSourceCode: &inlineSource},
		{Id: "storefront-deployment", VersionId: "1.1.0", ModuleSource: externalGitSource},
	}

	selected, err := selectInlineModules([]string{"checkout@1.0.0", "checkout@1.0.0"}, modules)
	require.NoError(t, err)
	require.Equal(t, []platformorchestratorcp.InternalModuleCatalogueModule{modules[0]}, selected)

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
	valid := platformorchestratorcp.InternalModuleCatalogueModule{Id: "checkout", VersionId: "1.0.0", ModuleSource: "inline", ModuleSourceCode: &source}
	for _, test := range []struct {
		name    string
		modules []platformorchestratorcp.InternalModuleCatalogueModule
	}{
		{name: "missing", modules: nil},
		{name: "duplicate", modules: []platformorchestratorcp.InternalModuleCatalogueModule{valid, valid}},
		{name: "missing source", modules: []platformorchestratorcp.InternalModuleCatalogueModule{{Id: "checkout", VersionId: "1.0.0", ModuleSource: "inline"}}},
		{name: "wrong source kind", modules: []platformorchestratorcp.InternalModuleCatalogueModule{{Id: "checkout", VersionId: "1.0.0", ModuleSource: "registry", ModuleSourceCode: &source}}},
		{name: "empty source", modules: []platformorchestratorcp.InternalModuleCatalogueModule{{Id: "checkout", VersionId: "1.0.0", ModuleSource: "inline", ModuleSourceCode: new(string)}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := selectInlineModules([]string{"checkout@1.0.0"}, test.modules)
			require.Error(t, err)
		})
	}
}
