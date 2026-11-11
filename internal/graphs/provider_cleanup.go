package graphs

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/hashicorp/hcl/v2/hclwrite"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/zclconf/go-cty/cty"
)

// ProviderSnapshot is the configuration of a deployment that may have written
// the current state. A failed apply may still need its deleted nodes' bindings.
type ProviderSnapshot struct {
	Graph, Tofu  []byte
	RunnerID     string
	MayBePartial bool
}

type cleanupProvider struct {
	id, variant string
	block       *hclwrite.Block
	backend     *hclwrite.Block
	requirement hclwrite.Tokens
	subs        map[string]map[string]graph.PlaceholderSub
}

type providerConfiguration struct {
	file         *hclwrite.File
	backend      *hclwrite.Block
	requirements *hclwrite.Body
	providers    map[string]*hclwrite.Block
	variables    map[string]*hclwrite.Block
}

// RetainProviderBindings adds only provider material to the compiled target.
// Historical HCL is evidence of the exact address and configuration; additions
// are rendered from currently authorized material, not replayed from history.
func RetainProviderBindings(target []byte, desired *graph.Graph[*GraphNodeModuleConfig], history []ProviderSnapshot, runnerID string,
	lookup ContextPlaceholderLookup, loadProviders func([]string) ([]cp.ModuleProvider, error),
) ([]byte, bool, error) {
	bindings, err := cleanupProviders(desired, history, runnerID)
	if err != nil || len(bindings) == 0 {
		return target, false, err
	}
	compiled, err := parseProviderConfiguration(target)
	if err != nil {
		return nil, false, err
	}
	var missing []string
	ids := make(map[string]bool)
	for _, alias := range slices.Sorted(maps.Keys(bindings)) {
		old := bindings[alias]
		if !sameBackend(old.backend, compiled.backend) {
			return nil, false, fmt.Errorf("state backend has changed since provider %s managed its resources", old.id)
		}
		if block := compiled.providers[alias]; block != nil {
			if !sameProviderBinding(old, block, compiled.requirements.GetAttribute(alias)) {
				return nil, false, fmt.Errorf("provider %s has a different configuration at its state address", old.id)
			}
			if err := compiled.addCleanupVariables(block, old.id); err != nil {
				return nil, false, err
			}
			continue
		}
		missing = append(missing, alias)
		ids[old.id] = true
	}
	if len(missing) == 0 {
		return compiled.file.Bytes(), true, nil
	}
	providers, err := loadProviders(slices.Sorted(maps.Keys(ids)))
	if err != nil {
		return nil, false, err
	}
	index := BuildProviderIndex(providers)
	for _, alias := range missing {
		old := bindings[alias]
		provider, ok := index[old.id]
		if !ok {
			return nil, false, fmt.Errorf("provider %s is no longer available", old.id)
		}
		block, err := renderCleanupProvider(alias, old, provider.ModuleProvider, lookup)
		if err != nil {
			return nil, false, err
		}
		if err := compiled.addCleanupVariables(block, old.id); err != nil {
			return nil, false, err
		}
		if existing := compiled.requirements.GetAttribute(alias); existing != nil && !sameHCL(existing.Expr().BuildTokens(nil), old.requirement) {
			return nil, false, fmt.Errorf("provider %s has incompatible requirements at its state address", old.id)
		}
		compiled.requirements.SetAttributeRaw(alias, old.requirement)
		compiled.file.Body().AppendNewline()
		compiled.file.Body().AppendBlock(block)
	}
	return compiled.file.Bytes(), true, nil
}

type historicalBinding struct {
	alias string
	cleanupProvider
}

func cleanupProviders(desired *graph.Graph[*GraphNodeModuleConfig], history []ProviderSnapshot, runnerID string) (map[string]cleanupProvider, error) {
	// Select aliases that can outlive a module change. Then verify every
	// historical use of those aliases, including uses at unchanged coordinates.
	candidates := make(map[string]bool)
	witnesses := make([][]historicalBinding, len(history))
	for index, snapshot := range history {
		previous, err := FromJson(bytes.NewReader(snapshot.Graph))
		if err != nil || previous == nil {
			return nil, fmt.Errorf("cannot read the state deployment graph")
		}
		for coordinate, node := range previous.Nodes {
			old := node.ModuleConfiguration
			if old == nil || old.Deleted && !snapshot.MayBePartial {
				continue
			}
			next := desired.Nodes[coordinate].ModuleConfiguration
			for id, variant := range old.ProviderFullIdToHashVariantMapping {
				alias, err := historicalProviderAlias(id, variant)
				if err != nil {
					return nil, err
				}
				if !usesProviderBinding(next, id, variant) {
					candidates[alias] = true
				}
				witnesses[index] = append(witnesses[index], historicalBinding{
					alias: alias, cleanupProvider: cleanupProvider{id: id, variant: variant, subs: old.ProviderSubsMap},
				})
			}
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	result := make(map[string]cleanupProvider, len(candidates))
	for index, bindings := range witnesses {
		var compiled *providerConfiguration
		for _, witness := range bindings {
			alias, old := witness.alias, witness.cleanupProvider
			if !candidates[alias] {
				continue
			}
			if history[index].RunnerID != runnerID {
				return nil, fmt.Errorf("provider %s requires the Runner that last managed its resources", old.id)
			}
			if compiled == nil {
				var err error
				compiled, err = parseProviderConfiguration(history[index].Tofu)
				if err != nil {
					return nil, err
				}
			}
			block, requirement := compiled.providers[alias], compiled.requirements.GetAttribute(alias)
			if block == nil || requirement == nil || !sameHCL(block.Body().GetAttribute("alias").Expr().BuildTokens(nil), hclwrite.TokensForValue(cty.StringVal(alias))) {
				return nil, fmt.Errorf("historical configuration for provider %s is unavailable", old.id)
			}
			if existing, ok := result[alias]; ok {
				if existing.id != old.id || existing.variant != old.variant || !sameBackend(existing.backend, compiled.backend) || !sameProviderBinding(existing, block, requirement) {
					return nil, fmt.Errorf("state deployments disagree about provider %s", old.id)
				}
				continue
			}
			old.block, old.backend, old.requirement = block, compiled.backend, requirement.Expr().BuildTokens(nil)
			result[alias] = old
		}
	}
	for _, node := range desired.Nodes {
		if node.ModuleConfiguration == nil {
			continue
		}
		for id, variant := range node.ModuleConfiguration.ProviderFullIdToHashVariantMapping {
			alias, err := historicalProviderAlias(id, variant)
			if err != nil {
				return nil, err
			}
			if old, ok := result[alias]; ok && (old.id != id || old.variant != variant) {
				return nil, fmt.Errorf("provider %s conflicts with a target binding at its state address", old.id)
			}
		}
	}
	return result, nil
}

func historicalProviderAlias(id, variant string) (string, error) {
	providerType, providerID, ok := strings.Cut(id, ".")
	if !ok || providerType == "" || providerID == "" || len(variant) < 8 {
		return "", fmt.Errorf("invalid provider binding")
	}
	return LocalProviderName(providerType, providerID, variant), nil
}

// A deleted node can still emit its provider. Leave that existing whole-node
// removal path alone when it already provides the same binding.
func usesProviderBinding(next *GraphNodeModuleConfig, id, variant string) bool {
	if next == nil || next.Definition == nil || next.ProviderFullIdToHashVariantMapping[id] != variant {
		return false
	}
	for _, providerID := range next.Definition.ProviderMapping {
		if providerID == id {
			return true
		}
	}
	return false
}

func parseProviderConfiguration(raw []byte) (*providerConfiguration, error) {
	file, diagnostics := hclwrite.ParseConfig(raw, "deployment.tf", hcl.InitialPos)
	if diagnostics.HasErrors() {
		return nil, fmt.Errorf("stored deployment configuration cannot be read")
	}
	terraform := file.Body().FirstMatchingBlock("terraform", nil)
	if terraform == nil {
		return nil, fmt.Errorf("deployment configuration has no terraform block")
	}
	requirements := terraform.Body().FirstMatchingBlock("required_providers", nil)
	if requirements == nil {
		return nil, fmt.Errorf("deployment configuration has no provider requirements")
	}
	out := &providerConfiguration{file: file, requirements: requirements.Body(), providers: map[string]*hclwrite.Block{}, variables: map[string]*hclwrite.Block{}}
	for _, block := range terraform.Body().Blocks() {
		if block.Type() == "backend" {
			if out.backend != nil {
				return nil, fmt.Errorf("deployment configuration has ambiguous state backends")
			}
			out.backend = block
		}
	}
	for _, block := range file.Body().Blocks() {
		var index map[string]*hclwrite.Block
		switch block.Type() {
		case "provider":
			index = out.providers
		case "variable":
			index = out.variables
		default:
			continue
		}
		labels := block.Labels()
		if len(labels) != 1 || index[labels[0]] != nil || block.Type() == "provider" && block.Body().GetAttribute("alias") == nil {
			return nil, fmt.Errorf("deployment configuration has ambiguous provider or variable declarations")
		}
		index[labels[0]] = block
	}
	return out, nil
}

func sameBackend(a, b *hclwrite.Block) bool {
	return a != nil && b != nil && sameHCL(a.BuildTokens(nil), b.BuildTokens(nil))
}

func sameProviderBinding(old cleanupProvider, block *hclwrite.Block, requirement *hclwrite.Attribute) bool {
	return requirement != nil && sameHCL(old.block.BuildTokens(nil), block.BuildTokens(nil)) && sameHCL(old.requirement, requirement.Expr().BuildTokens(nil))
}

func sameHCL(a, b hclwrite.Tokens) bool {
	values := func(tokens hclwrite.Tokens) []string {
		var out []string
		for _, token := range tokens {
			if token.Type != hclsyntax.TokenNewline && token.Type != hclsyntax.TokenComment {
				out = append(out, string(token.Bytes))
			}
		}
		return out
	}
	return slices.Equal(values(a), values(b))
}

func renderCleanupProvider(alias string, old cleanupProvider, provider cp.ModuleProvider, lookup ContextPlaceholderLookup) (*hclwrite.Block, error) {
	requirement := hclwrite.TokensForValue(cty.ObjectVal(map[string]cty.Value{
		"source": cty.StringVal(provider.Source), "version": cty.StringVal(provider.VersionConstraint),
	}))
	if sameHCL(old.requirement, requirement) {
		for _, localRef := range slices.Sorted(maps.Keys(old.subs)) {
			subs := old.subs[localRef]
			if hashUniqueProviderConfiguration(provider.ProviderType, provider.Id, subs) != old.variant {
				continue
			}
			block := hclwrite.NewBlock("provider", []string{alias})
			block.Body().SetAttributeValue("alias", cty.StringVal(alias))
			if err := addProviderConfigurationMapToBlock(block, nil, provider.Configuration, subs, lookup); err == nil && sameHCL(old.block.BuildTokens(nil), block.BuildTokens(nil)) {
				return block, nil
			}
		}
	}
	return nil, fmt.Errorf("provider %s no longer matches the configuration that managed its resources", old.id)
}

func (c *providerConfiguration) addCleanupVariables(block *hclwrite.Block, providerID string) error {
	file, diagnostics := hclsyntax.ParseConfig(block.BuildTokens(nil).Bytes(), "provider.tf", hcl.InitialPos)
	if diagnostics.HasErrors() {
		return fmt.Errorf("provider %s has invalid historical inputs", providerID)
	}
	variables := make(map[string]bool)
	if err := collectCleanupVariables(file.Body.(*hclsyntax.Body), variables); err != nil {
		return fmt.Errorf("provider %s needs historical inputs that cannot safely be reconstructed", providerID)
	}
	for _, name := range slices.Sorted(maps.Keys(variables)) {
		declaration := hclwrite.NewBlock("variable", []string{name})
		declaration.Body().SetAttributeRaw("type", hclwrite.TokensForIdentifier("string"))
		declaration.Body().SetAttributeValue("sensitive", cty.BoolVal(true))
		if existing := c.variables[name]; existing != nil {
			if !sameHCL(existing.BuildTokens(nil), declaration.BuildTokens(nil)) {
				return fmt.Errorf("provider %s has an incompatible historical variable declaration", providerID)
			}
			continue
		}
		c.file.Body().AppendNewline()
		c.file.Body().AppendBlock(declaration)
		c.variables[name] = declaration
	}
	return nil
}

func collectCleanupVariables(body *hclsyntax.Body, names map[string]bool) error {
	for _, attribute := range body.Attributes {
		for _, traversal := range attribute.Expr.Variables() {
			if len(traversal) != 2 || traversal.RootName() != "var" {
				return fmt.Errorf("non-variable reference")
			}
			variable, ok := traversal[1].(hcl.TraverseAttr)
			if !ok {
				return fmt.Errorf("unsupported variable reference")
			}
			names[variable.Name] = true
		}
	}
	for _, nested := range body.Blocks {
		if err := collectCleanupVariables(nested.Body, names); err != nil {
			return err
		}
	}
	return nil
}

// StateProviderIDs conservatively identifies Providers still associated with
// managed state. This includes all possibly partial attempts, not just nodes
// observed by a Runner before it failed.
func StateProviderIDs(history []ProviderSnapshot) ([]string, error) {
	ids, aliases := make(map[string]bool), make(map[string]bool)
	for _, snapshot := range history {
		g, err := FromJson(bytes.NewReader(snapshot.Graph))
		if err != nil || g == nil {
			return nil, fmt.Errorf("state graph is unavailable")
		}
		for _, node := range g.Nodes {
			config := node.ModuleConfiguration
			if config == nil || config.Deleted && !snapshot.MayBePartial {
				continue
			}
			for id, variant := range config.ProviderFullIdToHashVariantMapping {
				alias, err := historicalProviderAlias(id, variant)
				if err != nil {
					return nil, err
				}
				ids[id], aliases[alias] = true, true
			}
		}
	}
	for _, snapshot := range history {
		compiled, err := parseProviderConfiguration(snapshot.Tofu)
		if err != nil {
			return nil, err
		}
		parsed, diagnostics := hclsyntax.ParseConfig(snapshot.Tofu, "state.tf", hcl.InitialPos)
		if diagnostics.HasErrors() {
			return nil, fmt.Errorf("state configuration is unavailable")
		}
		for _, block := range parsed.Body.(*hclsyntax.Body).Blocks {
			if block.Type != "module" {
				continue
			}
			if mapping := block.Body.Attributes["providers"]; mapping != nil {
				for _, reference := range mapping.Expr.Variables() {
					if !aliases[reference.RootName()] {
						return nil, fmt.Errorf("state module has an unidentified provider binding")
					}
				}
			}
		}

		for alias := range compiled.providers {
			// Successfully removed bindings may remain only in cleanup HCL.
			// A partial attempt cannot establish that they were removed.
			if snapshot.MayBePartial && !aliases[alias] {
				return nil, fmt.Errorf("partial state has an unidentified provider binding")
			}
		}
	}
	return slices.Sorted(maps.Keys(ids)), nil
}
