package graphs

import (
	"bytes"
	"embed"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"testing"

	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/legacy-provider-subs/*.gob
var historicalProviderFixtures embed.FS

func legacyProviderFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := historicalProviderFixtures.ReadFile("testdata/legacy-provider-subs/" + name + ".gob")
	require.NoError(t, err)
	return raw
}

func historicalProviderValues() map[string]graph.PlaceholderSub {
	return map[string]graph.PlaceholderSub{
		"context":  &graph.ContextPlaceholder{Key: "fixture-context"},
		"tfvar":    &graph.TfVarPlaceholder{Key: "fixture-variable"},
		"resource": &graph.ResourcePlaceholder{Coordinate: &graph.ResourceCoordinate{Type: "fixture-resource", Class: "fixture-class", Id: "fixture-id"}, Alias: "fixture-alias", IsShared: true, IsSelf: true, Output: []string{"nested", "output"}},
		"selector": &graph.SelectorPlaceholder{
			MatchedCoordinates: []graph.ResourceCoordinate{{Type: "fixture-resource", Class: "fixture-class", Id: "fixture-id"}, {Type: "second-type", Class: "second-class", Id: "second-id"}},
			Functions:          []string{"first", "last"},
			Args:               []string{"one", "two"},
			Output:             []string{"selector", "value"},
		},
	}
}

func TestHistoricalProviderSubsFrozenFixtures(t *testing.T) {
	for _, name := range []string{"historical-map", "historical-canonical", "mixed-map", "mixed-canonical"} {
		t.Run(name, func(t *testing.T) {
			expected := historicalProviderValues()
			want := map[string]map[string]graph.PlaceholderSub{"fixture-provider": expected, "empty": {}}
			if name == "mixed-map" {
				delete(want, "empty")
				expected["current"] = &graph.TfVarPlaceholder{Key: "current-variable"}
			}
			if name == "mixed-canonical" {
				want = map[string]map[string]graph.PlaceholderSub{"fixture-provider": {"resource": expected["resource"], "current": &graph.TfVarPlaceholder{Key: "current-variable"}}}
			}
			actual, err := decodeProviderSubsMap(legacyProviderFixture(t, name))
			require.NoError(t, err)
			require.Equal(t, want, actual)
			canonical, err := encodeProviderSubsMap(actual)
			require.NoError(t, err)
			decoded, err := decodeProviderSubsMap(canonical)
			require.NoError(t, err)
			require.Equal(t, want, decoded)
			again, err := encodeProviderSubsMap(decoded)
			require.NoError(t, err)
			require.Equal(t, canonical, again)
			require.NotContains(t, string(canonical), "canyon_graph")
			// JSON decoding is also used directly by rollback and retained plan reads.
			config := []byte(
				`{"definition_id":"synthetic-module","version_id":"synthetic-version","has_inline_source":true,"deleted":true,"provider_full_id_to_hash_variant_mapping":{"fixture":"variant"},"provider_subs_map":"` + base64.StdEncoding.EncodeToString(legacyProviderFixture(t, name)) + `"}`,
			)
			var stored GraphNodeModuleConfig
			require.NoError(t, json.Unmarshal(config, &stored))
			require.Equal(t, want, stored.ProviderSubsMap)
			require.Equal(t, "synthetic-module", stored.DefinitionId)
			require.True(t, stored.Deleted)
			require.True(t, stored.HasInlineSource)
			output, err := json.Marshal(&stored)
			require.NoError(t, err)
			var reread GraphNodeModuleConfig
			require.NoError(t, json.Unmarshal(output, &reread))
			require.Equal(t, stored, reread)
			graphJSON := []byte(`{"nodes":{"type=workload,class=default,id=synthetic":{"config":` + string(config) + `,"params":{"example":"unchanged"}}}}`)
			parsed, err := FromJson(bytes.NewReader(graphJSON))
			require.NoError(t, err)
			node := parsed.Nodes[graph.ResourceCoordinate{Type: "workload", Class: "default", Id: "synthetic"}]
			require.Equal(t, want, node.ModuleConfiguration.ProviderSubsMap)
			require.Equal(t, map[string]any{"example": "unchanged"}, node.Params)
			rewritten, err := ToJson(parsed)
			require.NoError(t, err)
			reparsed, err := FromJson(bytes.NewReader(rewritten))
			require.NoError(t, err)
			require.Equal(t, parsed, reparsed)
		})
	}
}

func TestHistoricalProviderSubsRejectInvalid(t *testing.T) {
	_, err := decodeProviderSubsMap(legacyProviderFixture(t, "unsupported-version"))
	require.ErrorContains(t, err, "unsupported ProviderSubsMap encoding version 99")
	for _, raw := range [][]byte{nil, []byte("not gob"), legacyProviderFixture(t, "unknown-type"), legacyProviderFixture(t, "historical-map")[:30]} {
		_, err := decodeProviderSubsMap(raw)
		require.Error(t, err)
	}
}

type unsupportedProviderPlaceholder struct{ Key string }

func (*unsupportedProviderPlaceholder) Type() graph.PlaceholderType { return "unsupported" }

func TestHistoricalProviderSubsRejectRegisteredUnsupportedType(t *testing.T) {
	gob.Register(&unsupportedProviderPlaceholder{})
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(map[string]map[string]graph.PlaceholderSub{"fixture": {"unknown": &unsupportedProviderPlaceholder{Key: "synthetic"}}}))
	_, err := decodeProviderSubsMap(encoded.Bytes())
	require.ErrorContains(t, err, "unsupported ProviderSubsMap placeholder type")
}

func TestHistoricalProviderSubsIntegrationFixture(t *testing.T) {
	actual, err := decodeProviderSubsMap(legacyProviderFixture(t, "historical-tfvar-integration"))
	require.NoError(t, err)
	require.Equal(t, map[string]map[string]graph.PlaceholderSub{"faulty": {"${var.legacy_provider_flag}": &graph.TfVarPlaceholder{Key: "legacy_provider_flag"}}}, actual)
}
