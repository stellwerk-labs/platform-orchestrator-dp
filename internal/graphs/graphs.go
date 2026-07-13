package graphs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
)

func init() {
	// Register all placeholder types with gob for serialization
	gob.Register(&platform_orchestrator_graph.ContextPlaceholder{})
	gob.Register(&platform_orchestrator_graph.TfVarPlaceholder{})
	gob.Register(&platform_orchestrator_graph.ResourcePlaceholder{})
	gob.Register(&platform_orchestrator_graph.SelectorPlaceholder{})
	gob.Register(&platform_orchestrator_graph.ResourceCoordinate{})
}

// GraphNodeModuleConfig is the struct we store on each of the nodes in the graph linking it to its definition.
// We store some fields as JSON serializable so that we can store which definition version is used for the node and
// then the Definition field is used only during graph and terraform creation to lookup dependencies, module source,
// params and other fields from the active definition.
type GraphNodeModuleConfig struct {
	DefinitionId string `json:"definition_id"`
	VersionId    string `json:"version_id"`
	// HasInlineSource is an optimisation field that allows to efficiently gather the source of the inline modules
	// from a graph when we are constructing the TF bundle. Without this, we would need to pull all modules, or
	// include all the module sources in the TF file stored in the DB which could be fairly large.
	HasInlineSource bool `json:"has_inline_source"`

	// Deleted allows us to hold a node in the graph that should not generate terraform code, but has providers that
	// need to still be included in the graph.
	Deleted bool `json:"deleted,omitempty"`

	// ProviderFullIdToHashVariantMapping maps from the local provider name used in the terraform module to the variant used to identify the provider uniquely.
	ProviderFullIdToHashVariantMapping map[string]string `json:"provider_full_id_to_hash_variant_mapping,omitempty"`

	// ProviderSubsMap holds the resolved substitutions for each provider local ref used by this module, to be used when building the tofu.
	ProviderSubsMap map[string]map[string]platform_orchestrator_graph.PlaceholderSub `json:"provider_subs_map,omitempty"`

	Definition *platformorchestratorcp.InternalModuleCatalogueModule `json:"-"`
}

// MarshalJSON implements custom JSON marshaling for GraphNodeModuleConfig
func (g *GraphNodeModuleConfig) MarshalJSON() ([]byte, error) {
	type Alias GraphNodeModuleConfig

	// Encode ProviderSubsMap as base64-encoded gob blob
	var encodedSubsMap string
	if g.ProviderSubsMap != nil {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(g.ProviderSubsMap); err != nil {
			return nil, fmt.Errorf("failed to encode ProviderSubsMap: %w", err)
		}
		encodedSubsMap = base64.StdEncoding.EncodeToString(buf.Bytes())
	}

	return json.Marshal(&struct {
		*Alias
		ProviderSubsMap string `json:"provider_subs_map,omitempty"`
	}{
		Alias:           (*Alias)(g),
		ProviderSubsMap: encodedSubsMap,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for GraphNodeModuleConfig
func (g *GraphNodeModuleConfig) UnmarshalJSON(data []byte) error {
	type Alias GraphNodeModuleConfig

	aux := &struct {
		*Alias
		ProviderSubsMap string `json:"provider_subs_map,omitempty"`
	}{
		Alias: (*Alias)(g),
	}

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	// Decode ProviderSubsMap from base64-encoded gob blob
	if aux.ProviderSubsMap != "" {
		decoded, err := base64.StdEncoding.DecodeString(aux.ProviderSubsMap)
		if err != nil {
			return fmt.Errorf("failed to decode ProviderSubsMap: %w", err)
		}

		dec := gob.NewDecoder(bytes.NewReader(decoded))
		if err := dec.Decode(&g.ProviderSubsMap); err != nil {
			return fmt.Errorf("failed to decode ProviderSubsMap: %w", err)
		}
	}

	return nil
}

// CalculateDrift is part of the interface needed by the platform_orchestrator_graph library. This is used to compare the difference
// between rules used to generate a previous graph for the environment, and the current rules for the environment in
// order to determine a "drift" from the previous definition and the current rule set. We store the resulting drift
// in the graph and can analyse this to notify platform engineers where resources needed to be updated to the latest
// definition version.
func (g *GraphNodeModuleConfig) CalculateDrift(mc platform_orchestrator_graph.ModuleConfiguration) platform_orchestrator_graph.DriftType {
	if g2, ok := mc.(*GraphNodeModuleConfig); ok {
		if g2.DefinitionId != g.DefinitionId {
			return platform_orchestrator_graph.DriftModuleChange
		} else if g2.VersionId != g.VersionId {
			return platform_orchestrator_graph.DriftModuleChange
		}
		return platform_orchestrator_graph.DriftNone
	}
	panic("incorrect types")
}

// GetDependencies returns the dependencies of the node based on its module configuration. This is used during graph
// generation.
func (g *GraphNodeModuleConfig) GetDependencies() map[string]platform_orchestrator_graph.ManifestResource {
	if g.Definition == nil {
		panic(fmt.Sprintf("module definition is nil on %s@%s", g.DefinitionId, g.VersionId))
	}
	return maps.Collect(func(yield func(string, platform_orchestrator_graph.ManifestResource) bool) {
		for k, v := range g.Definition.Dependencies {
			yield(k, platform_orchestrator_graph.ManifestResource{
				Type:   v.Type,
				Class:  platform_orchestrator_graph.OptionalStringOfRef(v.Class),
				Id:     platform_orchestrator_graph.OptionalStringOfRef(v.Id),
				Params: maps.Clone(v.Params),
			})
		}
	})
}

// GetCoProvisioned is implemented by the platformorchestrator graph library, but we don't define coprovisioning yet in the dataplane
// API.
func (g *GraphNodeModuleConfig) GetCoProvisioned() []platform_orchestrator_graph.ManifestCoProvision {
	if g.Definition == nil {
		panic(fmt.Sprintf("module definition is nil on %s@%s", g.DefinitionId, g.VersionId))
	}
	return slices.Collect(func(yield func(provision platform_orchestrator_graph.ManifestCoProvision) bool) {
		for _, v := range g.Definition.Coprovisioned {
			yield(platform_orchestrator_graph.ManifestCoProvision{
				ManifestResource: platform_orchestrator_graph.ManifestResource{
					Type:   v.Type,
					Class:  platform_orchestrator_graph.OptionalStringOfRef(v.Class),
					Id:     platform_orchestrator_graph.OptionalStringOfRef(v.Id),
					Params: maps.Clone(v.Params),
				},
				IsDependentOnCurrent:    v.IsDependentOnCurrent,
				CopyDependentsOnCurrent: v.CopyDependentsFromCurrent,
			})
		}
	})
}

// FindPinnedDefinitions returns all the unique module definition versions found in the graph. These are the "pinned"
// versions and are likely to be needed if the graph drift resolution indicates that a node should continue using
// the old version.
func FindPinnedDefinitions(g *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]) []string {
	seen := make(map[string]bool, len(g.Nodes))
	for _, r := range g.Nodes {
		if r.ModuleConfiguration != nil {
			seen[fmt.Sprintf("%s@%s", r.ModuleConfiguration.DefinitionId, r.ModuleConfiguration.VersionId)] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// buildModuleDefinitionIndex converts the catalogue of module definitions from the control plane into a definition index
// needed by the platform orchestrator graph library.
func buildModuleDefinitionIndex(definitions []platformorchestratorcp.InternalModuleCatalogueModule) *platform_orchestrator_graph.ModuleDefinitionIndex[*GraphNodeModuleConfig] {
	out := make([]platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig], 0, len(definitions))
	for _, d := range definitions {
		rules := make([]platform_orchestrator_graph.Rule, 0, len(d.Rules))
		for _, rule := range d.Rules {
			rules = append(rules, platform_orchestrator_graph.Rule{
				ResourceClass: platform_orchestrator_graph.OptionalStringOf(rule.ResourceClass),
				ResourceId:    platform_orchestrator_graph.OptionalStringOfRef(rule.ResourceId),
			})
		}
		out = append(out, platform_orchestrator_graph.ModuleDefinition[*GraphNodeModuleConfig]{
			ResourceType: d.ResourceType,
			Configuration: &GraphNodeModuleConfig{
				DefinitionId:    d.Id,
				VersionId:       d.VersionId,
				Definition:      &d,
				HasInlineSource: d.ModuleSourceCode != nil,
			},
			Rules: rules,
		})
	}
	return platform_orchestrator_graph.NewModuleDefinitionIndex(out)
}

// AssignModuleDefinitionsToGraphNodes assigns the pinned definition versions to the nodes of the last graph so that
// the platform-orchestrator graph library can identify and resolve definition drift.
func AssignModuleDefinitionsToGraphNodes(g *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], moduleDefinitions []platformorchestratorcp.InternalModuleCatalogueModule) int {
	type key struct {
		D, V string
	}
	missing := 0
	lookupTable := make(map[key]int)
	for i, definition := range moduleDefinitions {
		lookupTable[key{definition.Id, definition.VersionId}] = i
	}
	for coordinate, n := range g.Nodes {
		if n.ModuleConfiguration != nil {
			i, ok := lookupTable[key{n.ModuleConfiguration.DefinitionId, n.ModuleConfiguration.VersionId}]
			if !ok {
				// accept the destruction and replacement that will ensue
				n.NextDriftResolution = platform_orchestrator_graph.DriftResolutionAcceptModuleChange
				n.Error = new(platform_orchestrator_graph.ErrNoRuleMatch)
				missing++
			} else {
				n.ModuleConfiguration.Definition = &moduleDefinitions[i]
				n.ModuleConfiguration.HasInlineSource = moduleDefinitions[i].ModuleSourceCode != nil
				// NOTE: also specify that we are ok with the node changing its entire definition and being replaced. This
				// needs to be removed or made more clever in the future when we want platform engineers to negotiation the
				// drift between pinned versions and current rules.
				n.NextDriftResolution = platform_orchestrator_graph.DriftResolutionAcceptModuleChange
			}
			g.Nodes[coordinate] = n
		}
	}
	return missing
}

func BuildGraph(ctx context.Context, manifest platform_orchestrator_graph.Manifest, moduleDefinitions []platformorchestratorcp.InternalModuleCatalogueModule, lastGraph *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]) (*platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], error) {
	if lastGraph != nil {
		// Assign pinned definitions to the graph to determine drift resolution
		// we don't care about missing versions at this point because these are only for the drift calculation.
		_ = AssignModuleDefinitionsToGraphNodes(lastGraph, moduleDefinitions)
	}

	newGraph, err := platform_orchestrator_graph.SeedAndExpandAll(ctx, manifest, *buildModuleDefinitionIndex(moduleDefinitions), lastGraph)
	if err != nil {
		return nil, errors.Wrap(err, "failed to seed and expand graph")
	}

	if err := validateModuleParams(ctx, &newGraph); err != nil {
		return nil, err
	}

	return &newGraph, nil
}

// AddDeletedNodes adds deleted nodes back into the graph, flagged as Deleted=true. If includeDeletedFromLastGraph is
// true, then we also copy nodes flagged deleted in the previous graph which should be used if the previous deployment
// failed because we don't know whether the node was deleted successfully or not.
func AddDeletedNodes(newGraph *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], lastGraph *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], includeDeletedFromLastGraph bool) {
	if lastGraph != nil {
		for rc := range lastGraph.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePreOrder) {
			// if the node is already in the new graph, skip it
			if _, ok := newGraph.Nodes[rc]; ok {
				continue
			}
			n := lastGraph.Nodes[rc]
			// The nodes must have module configuration. By default, we don't include nodes marked deleted in the previous
			// graph unless the flag says we must.
			if n.ModuleConfiguration != nil && (!n.ModuleConfiguration.Deleted || includeDeletedFromLastGraph) {
				// Clone the module configuration and set deleted=true
				newGraph.Nodes[rc] = platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]{
					ModuleConfiguration: &GraphNodeModuleConfig{
						DefinitionId:                       n.ModuleConfiguration.DefinitionId,
						VersionId:                          n.ModuleConfiguration.VersionId,
						Deleted:                            true,
						Definition:                         n.ModuleConfiguration.Definition,
						ProviderFullIdToHashVariantMapping: n.ModuleConfiguration.ProviderFullIdToHashVariantMapping,
						ProviderSubsMap:                    n.ModuleConfiguration.ProviderSubsMap,
					},
				}
				newGraph.Edges[rc] = lastGraph.Edges[rc]
			}
		}
	}
}

// BuildGraphDistanceMatrix builds the adjacency matrix, fills in resource parameter dependencies,
// and returns the computed distance matrix. This matrix is needed by both AddProviderMappingToNodes
// and BuildTofuFromGraph to resolve resource substitutions.
func BuildGraphDistanceMatrix(g *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]) platform_orchestrator_graph.DistanceMatrix {
	matrix := g.BuildAdjacencyMatrix()
	fillInResourceParamDependencies(g, matrix)
	return matrix.FillDistanceMatrix()
}

func AddProviderMappingToNodes(g *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], providers []platformorchestratorcp.ModuleProvider, matrix platform_orchestrator_graph.DistanceMatrix) error {
	provIndex := BuildProviderIndex(providers)

	// Now go through the graph and resolve provider configurations for each module node
	for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
		if rc.Type == platform_orchestrator_graph.DefaultWorkloadResourceType {
			continue
		}

		n := g.Nodes[rc]
		if n.ModuleConfiguration == nil || n.ModuleConfiguration.Definition == nil {
			return fmt.Errorf("module configuration is nil on node %s (%v)", rc.String(), n)
		}

		if pm := n.ModuleConfiguration.Definition.ProviderMapping; len(pm) > 0 {
			// Create a new ModuleConfiguration for this node to avoid sharing state between nodes
			n.ModuleConfiguration = &GraphNodeModuleConfig{
				DefinitionId:                       n.ModuleConfiguration.DefinitionId,
				VersionId:                          n.ModuleConfiguration.VersionId,
				HasInlineSource:                    n.ModuleConfiguration.HasInlineSource,
				Deleted:                            n.ModuleConfiguration.Deleted,
				Definition:                         n.ModuleConfiguration.Definition,
				ProviderFullIdToHashVariantMapping: make(map[string]string),
				ProviderSubsMap:                    make(map[string]map[string]platform_orchestrator_graph.PlaceholderSub),
			}
			for _, localRef := range slices.Sorted(maps.Keys(pm)) {
				pi, ok := provIndex[pm[localRef]]
				if !ok {
					return UserBadRequestError(fmt.Sprintf(
						"unknown provider '%s' used by module definition %s@%s for %s", pm[localRef], n.ModuleConfiguration.DefinitionId, n.ModuleConfiguration.VersionId, rc.String(),
					))
				}

				var err error
				subs := maps.Collect(platform_orchestrator_graph.IterPlaceholders(pi.Configuration, &err))
				if err != nil {
					return UserBadRequestError(fmt.Sprintf("failed to parse placeholders for provider configuration for provider '%s': %v", pi.Id, err))
				}

				if err = resolveResourceSubstitutions(g, matrix, rc, rc, subs); err != nil {
					var moduleSnippet string
					if rc.Type != "" {
						moduleSnippet = fmt.Sprintf(" for module '%s@%s'", n.ModuleConfiguration.DefinitionId, n.ModuleConfiguration.VersionId)
					}
					return UserBadRequestError(fmt.Sprintf("provider '%s.%s'%s in context of node '%s': failed to resolve placeholders: %v", pi.ProviderType, pi.Id, moduleSnippet, rc, err))
				}

				// Now we may have already generated a copy of the provider with the same resolved configuration. We can detect this by
				// looking at a unique hash of the provider type, id, and substitutions.
				providerVariantHash := hashUniqueProviderConfiguration(pi.ProviderType, pi.Id, subs)
				// Store using full provider identifier (pm[localRef]) as key for enrichment purposes
				fullProviderIdentifier := pm[localRef]
				n.ModuleConfiguration.ProviderFullIdToHashVariantMapping[fullProviderIdentifier] = providerVariantHash
				n.ModuleConfiguration.ProviderSubsMap[localRef] = subs
			}
			g.Nodes[rc] = n
		}
	}
	return nil
}

func doesModuleParamTypeMatch(expectedType platformorchestratorcp.ModuleParamItemType, raw interface{}) bool {
	typed := reflect.ValueOf(raw)
	// NOTE: we're expecting exact type matches here. We're not supporting coersion between strings, bools, and numbers
	// yet which Terraform does do (see https://developer.hashicorp.com/terraform/language/expressions/types#type-conversion)
	// so we're a bit stricter than we otherwise have to be.
	switch expectedType {
	case platformorchestratorcp.Number:
		// just use the json number types here
		return typed.Kind() == reflect.Float64 || typed.Kind() == reflect.Int64
	case platformorchestratorcp.Bool:
		return typed.Kind() == reflect.Bool
	case platformorchestratorcp.List:
		return typed.Kind() == reflect.Slice || typed.Kind() == reflect.Array
	case platformorchestratorcp.Map:
		return typed.Kind() == reflect.Map
	case platformorchestratorcp.String:
		return typed.Kind() == reflect.String
	case platformorchestratorcp.Any:
		return true
	default:
		// catch badly defined params
		return false
	}
}

func validateModuleParamInner(ctx context.Context, rc platform_orchestrator_graph.ResourceCoordinate, node *platform_orchestrator_graph.ResourceNode[*GraphNodeModuleConfig]) error {
	for key, def := range node.ModuleConfiguration.Definition.ModuleParams {
		if !def.IsOptional && node.Params[key] == nil {
			return UserBadRequestError(fmt.Sprintf("node '%s': uses module %s@%s which requires %s param '%s' to be set", rc, node.ModuleConfiguration.DefinitionId, node.ModuleConfiguration.VersionId, def.Type, key))
		}
		if node.Params[key] != nil {
			if v, ok := node.Params[key].(string); !ok && strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") {
				// this looks like an expression which could produce any type so we can't validate further
				continue
			} else if !doesModuleParamTypeMatch(def.Type, node.Params[key]) {
				return UserBadRequestError(fmt.Sprintf("node '%s': uses module %s@%s which expects param '%s' to be of type '%s' but got '%T'", rc, node.ModuleConfiguration.DefinitionId, node.ModuleConfiguration.VersionId, key, def.Type, node.Params[key]))
			}
		}
	}

	for k := range node.Params {
		if _, ok := node.ModuleConfiguration.Definition.ModuleParams[k]; !ok {
			return UserBadRequestError(fmt.Sprintf("node '%s': uses module %s@%s which does not define module_param '%s'", rc, node.ModuleConfiguration.DefinitionId, node.ModuleConfiguration.VersionId, k))
		}
	}

	return nil
}

// validateModuleParams runs through the graph and validates that the params on each node match the params supported in
// the linked module definition.
func validateModuleParams(ctx context.Context, g *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig]) error {
	for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePreOrder) {
		if node := g.Nodes[rc]; node.ModuleConfiguration != nil {
			if err := validateModuleParamInner(ctx, rc, &node); err != nil {
				return err
			}
		}
	}
	return nil
}
