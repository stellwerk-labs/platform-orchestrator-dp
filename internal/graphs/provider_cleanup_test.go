package graphs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclwrite"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"
)

const cleanupRunner = "fixture-runner"

func providerCleanupConfig(t *testing.T, version string, provider *cp.ModuleProvider) (*graph.Graph[*GraphNodeModuleConfig], []byte) {
	t.Helper()
	module := &cp.InternalExecutionCatalogueModule{Id: "fixture", VersionId: version, ModuleSource: "./" + version}
	var providers []cp.ModuleProvider
	if provider != nil {
		module.ProviderMapping = map[string]string{provider.ProviderType: provider.ProviderType + "." + provider.Id}
		providers = []cp.ModuleProvider{*provider}
	}
	index := *graph.NewModuleDefinitionIndex([]graph.ModuleDefinition[*GraphNodeModuleConfig]{{
		ResourceType: "fixture", Rules: []graph.Rule{{}}, Configuration: &GraphNodeModuleConfig{
			DefinitionId: module.Id, VersionId: version, Definition: module,
		},
	}})
	g, err := graph.SeedAndExpandAll(t.Context(), graph.Manifest{
		SharedResources: map[string]graph.ManifestResource{"fixture": {Type: "fixture"}},
	}, index, nil)
	require.NoError(t, err)
	matrix := BuildGraphDistanceMatrix(&g)
	require.NoError(t, AddProviderMappingToNodes(&g, providers, matrix))
	var storage cp.StateStorageConfiguration
	require.NoError(t, storage.FromK8sStorageConfiguration(cp.K8sStorageConfiguration{Namespace: "test"}))
	raw, err := BuildTofuFromGraph(&g, providers, cp.Runner{StateStorageConfiguration: storage}, uuid.Nil, ContextPlaceholderLookupMap(nil), matrix)
	require.NoError(t, err)
	return &g, raw
}

func cleanupSnapshot(t *testing.T, g *graph.Graph[*GraphNodeModuleConfig], raw []byte, partial bool) ProviderSnapshot {
	t.Helper()
	encoded, err := ToJson(g)
	require.NoError(t, err)
	return ProviderSnapshot{Graph: encoded, Tofu: raw, RunnerID: cleanupRunner, MayBePartial: partial}
}

func TestProviderCleanupRejectsChangedMaterial(t *testing.T) {
	provider := cp.ModuleProvider{
		ProviderType: "example", Id: "default", Source: "example/provider", VersionConstraint: "= 1.0.0",
		Configuration: map[string]interface{}{"endpoint": "old-endpoint", "input": "${var.fixture_input}"},
	}
	oldGraph, old := providerCleanupConfig(t, "old", &provider)
	targetGraph, target := providerCleanupConfig(t, "target", nil)
	history := []ProviderSnapshot{cleanupSnapshot(t, oldGraph, old, false)}
	load := func(ids []string) ([]cp.ModuleProvider, error) {
		require.Equal(t, []string{"example.default"}, ids)
		return []cp.ModuleProvider{provider}, nil
	}
	retained, used, err := RetainProviderBindings(target, targetGraph, history, cleanupRunner, nil, load)
	require.NoError(t, err)
	require.True(t, used)
	require.Contains(t, string(retained), `variable "fixture_input"`)
	require.Contains(t, string(retained), "sensitive = true")
	require.NotContains(t, string(retained), "default =")
	require.Contains(t, string(retained), "var.fixture_input")

	changed := provider
	changed.Configuration = map[string]interface{}{"endpoint": "new-endpoint", "input": "${var.fixture_input}"}
	_, _, err = RetainProviderBindings(target, targetGraph, history, cleanupRunner, nil, func([]string) ([]cp.ModuleProvider, error) {
		return []cp.ModuleProvider{changed}, nil
	})
	require.ErrorContains(t, err, "no longer matches")
	require.NotContains(t, err.Error(), "new-endpoint")

	_, _, err = RetainProviderBindings(target, targetGraph, history, "another-runner", nil, load)
	require.ErrorContains(t, err, "requires the Runner")
	otherBackend := bytes.Replace(target, []byte(`namespace         = "test"`), []byte(`namespace         = "other"`), 1)
	require.NotEqual(t, target, otherBackend)
	_, _, err = RetainProviderBindings(otherBackend, targetGraph, history, cleanupRunner, nil, load)
	require.ErrorContains(t, err, "state backend has changed")
	_, _, err = RetainProviderBindings(target, targetGraph, history, cleanupRunner, nil, func([]string) ([]cp.ModuleProvider, error) {
		return nil, nil
	})
	require.ErrorContains(t, err, "no longer available")
}

func TestProviderCleanupRejectsDisagreeingHistory(t *testing.T) {
	provider := cp.ModuleProvider{
		ProviderType: "example", Id: "default", Source: "example/provider", VersionConstraint: "= 1.0.0",
		Configuration: map[string]interface{}{"endpoint": "old-endpoint"},
	}
	oldGraph, old := providerCleanupConfig(t, "old", &provider)
	targetGraph, target := providerCleanupConfig(t, "target", nil)
	provider.Configuration = map[string]interface{}{"endpoint": "new-endpoint"}
	failedGraph, failed := providerCleanupConfig(t, "target", &provider)
	_, _, err := RetainProviderBindings(target, targetGraph, []ProviderSnapshot{
		cleanupSnapshot(t, failedGraph, failed, true),
		cleanupSnapshot(t, oldGraph, old, false),
	}, cleanupRunner, nil, nil)
	require.ErrorContains(t, err, "state deployments disagree")
}

// Run against a local mirror of the signed hashicorp/random 3.7.2 package.
// No network, infrastructure credentials, or remote state are needed.
func TestProviderCleanupWithRealState(t *testing.T) {
	binary, mirror := os.Getenv("DP_TEST_IAC_BINARY"), os.Getenv("DP_TEST_PROVIDER_MIRROR")
	if binary == "" || mirror == "" {
		t.Skip("set DP_TEST_IAC_BINARY and DP_TEST_PROVIDER_MIRROR to exercise real state")
	}
	registry := os.Getenv("DP_TEST_PROVIDER_REGISTRY")
	if registry == "" {
		registry = "registry.opentofu.org"
	}
	source := registry + "/hashicorp/random"
	provider := cp.ModuleProvider{ProviderType: "random", Id: "fixture", Source: source, VersionConstraint: "= 3.7.2"}
	providerFreeGraph, providerFree := providerCleanupConfig(t, "baseline", nil)
	oldGraph, old := providerCleanupConfig(t, "with-provider", &provider)

	for _, scenario := range []struct {
		name, targetVersion string
		partial, retry      bool
	}{
		{"upgrade", "next", false, false},
		{"rollback", "baseline", false, false},
		{"partial apply", "baseline", true, false},
		{"retry after failed cleanup", "baseline", false, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			dir := t.TempDir()
			root, err := os.OpenRoot(dir)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, root.Close()) })
			write := func(name, content string) {
				t.Helper()
				require.NoError(t, root.WriteFile(name, []byte(content), 0600))
			}
			for _, version := range []string{"baseline", "with-provider", "next"} {
				require.NoError(t, os.Mkdir(filepath.Join(dir, version), 0700))
			}
			write("cli.rc", fmt.Sprintf("provider_installation {\n filesystem_mirror { path = %q }\n}\n", mirror))
			for _, version := range []string{"baseline", "next"} {
				write(version+"/main.tf", `output "value" { value = "provider-free" }`)
			}
			module := fmt.Sprintf(`
terraform {
  required_providers {
    random = { source = %q }
  }
}
resource "random_id" "fixture" { byte_length = 4 }
`, source)
			if scenario.partial {
				module += `
resource "terraform_data" "failure" {
  depends_on = [random_id.fixture]
  provisioner "local-exec" {
    command = "exit 1"
  }
}
`
			}
			write("with-provider/main.tf", module)
			configure := func(raw []byte) {
				t.Helper()
				f, diagnostics := hclwrite.ParseConfig(raw, "main.tf", hcl.InitialPos)
				require.False(t, diagnostics.HasErrors(), "%s", diagnostics.Error())
				body := f.Body().FirstMatchingBlock("terraform", nil).Body()
				// Replace only the backend, using the real compiler output for all bindings.
				body.RemoveBlock(body.FirstMatchingBlock("backend", []string{"kubernetes"}))
				write("main.tf", string(f.Bytes()))
			}
			run := func(args ...string) ([]byte, error) {
				t.Helper()
				cmd := exec.CommandContext(t.Context(), binary, args...) // #nosec G204 G702 -- explicit operator-provided test binary
				cmd.Dir = dir
				cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "TF_CLI_CONFIG_FILE=" + filepath.Join(dir, "cli.rc"), "CHECKPOINT_DISABLE=1"}
				return cmd.CombinedOutput()
			}
			mustRun := func(args ...string) []byte {
				t.Helper()
				output, err := run(args...)
				require.NoError(t, err, "%v: %s", args, output)
				return output
			}
			configure(providerFree)
			mustRun("init", "-input=false", "-no-color")
			mustRun("apply", "-auto-approve", "-input=false", "-no-color")
			configure(old)
			mustRun("init", "-input=false", "-no-color")
			if scenario.partial {
				output, err := run("apply", "-auto-approve", "-input=false", "-no-color")
				require.Error(t, err)
				require.Contains(t, string(output), "local-exec provisioner error")
			} else {
				mustRun("apply", "-auto-approve", "-input=false", "-no-color")
			}
			require.Contains(t, string(mustRun("state", "list")), ".random_id.fixture")
			state, err := root.ReadFile("terraform.tfstate")
			require.NoError(t, err)

			targetGraph, target := providerCleanupConfig(t, scenario.targetVersion, nil)
			configure(target)
			mustRun("init", "-input=false", "-no-color")
			output, err := run("plan", "-input=false", "-no-color")
			require.Error(t, err)
			require.Contains(t, string(output), "Provider configuration not present")

			history := []ProviderSnapshot{cleanupSnapshot(t, oldGraph, old, scenario.partial)}
			if scenario.partial {
				history = append(history, cleanupSnapshot(t, providerFreeGraph, providerFree, false))
			}
			if scenario.retry {
				// The first cleanup attempt failed during plan. Its graph no
				// longer mentions the provider, but state still needs it.
				history = append([]ProviderSnapshot{cleanupSnapshot(t, targetGraph, target, true)}, history...)
			}
			load := func(ids []string) ([]cp.ModuleProvider, error) {
				require.Equal(t, []string{"random.fixture"}, ids)
				return []cp.ModuleProvider{provider}, nil
			}
			fixed, used, err := RetainProviderBindings(target, targetGraph, history, cleanupRunner, nil, load)
			require.NoError(t, err)
			require.True(t, used)
			configure(fixed)
			mustRun("init", "-input=false", "-no-color")
			mustRun("plan", "-input=false", "-no-color", "-out=cleanup.plan")
			var plan struct {
				ResourceChanges []struct {
					Address string
					Change  struct{ Actions []string }
				} `json:"resource_changes"`
			}
			require.NoError(t, json.Unmarshal(mustRun("show", "-json", "cleanup.plan"), &plan))
			want := 1
			if scenario.partial {
				want++
			}
			require.Len(t, plan.ResourceChanges, want)
			for _, change := range plan.ResourceChanges {
				require.Equal(t, []string{"delete"}, change.Change.Actions, change.Address)
			}
			afterPlan, err := root.ReadFile("terraform.tfstate")
			require.NoError(t, err)
			require.True(t, bytes.Equal(state, afterPlan), "plan changed state")
			mustRun("apply", "-input=false", "-no-color", "cleanup.plan")
			require.Empty(t, string(mustRun("state", "list")))

			next, used, err := RetainProviderBindings(target, targetGraph, []ProviderSnapshot{cleanupSnapshot(t, targetGraph, fixed, false)}, cleanupRunner, nil, load)
			require.NoError(t, err)
			require.False(t, used)
			require.Equal(t, target, next)
			configure(next)
			mustRun("init", "-input=false", "-no-color")
			require.Contains(t, string(mustRun("plan", "-input=false", "-no-color")), "No changes")
		})
	}
}

func TestStateProviderIDsRejectsMissingGraphBindings(t *testing.T) {
	provider := cp.ModuleProvider{ProviderType: "example", Id: "default", Source: "example/provider", VersionConstraint: "= 1.0.0"}
	bound, raw := providerCleanupConfig(t, "bound", &provider)
	empty, _ := providerCleanupConfig(t, "empty", nil)
	ids, err := StateProviderIDs([]ProviderSnapshot{cleanupSnapshot(t, bound, raw, false)})
	require.NoError(t, err)
	require.Equal(t, []string{"example.default"}, ids)
	_, err = StateProviderIDs([]ProviderSnapshot{cleanupSnapshot(t, empty, raw, false)})
	require.ErrorContains(t, err, "unidentified provider binding")
}
