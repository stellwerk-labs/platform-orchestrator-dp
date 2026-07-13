package graphs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/pkg/errors"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	platform_orchestrator_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
	"github.com/zclconf/go-cty/cty"
	"github.com/zclconf/go-cty/cty/gocty"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/util"
)

const (
	hclModuleRoot                      = "module"
	platformOrchestratorMetadataOutput = "platform_orchestrator_metadata"
)

var placeholdersSupportedInModule = []platform_orchestrator_graph.PlaceholderType{
	platform_orchestrator_graph.PlaceholderTypeContext,
	platform_orchestrator_graph.PlaceholderTypeResource,
	platform_orchestrator_graph.PlaceholderTypeSelector,
	platform_orchestrator_graph.PlaceholderTypeSelf,
}

var placeholdersSupportedInWorkload = []platform_orchestrator_graph.PlaceholderType{
	platform_orchestrator_graph.PlaceholderTypeContext,
	platform_orchestrator_graph.PlaceholderTypeResource,
}

var placeholdersSupportedInProvider = []platform_orchestrator_graph.PlaceholderType{
	platform_orchestrator_graph.PlaceholderTypeContext,
	platform_orchestrator_graph.PlaceholderTypeTfVar,
	platform_orchestrator_graph.PlaceholderTypeResource,
}

var blockConfigurationSuffixRegex = regexp.MustCompile(`[^\\]\[\d+]$`)

type ContextPlaceholderLookup func(key string) (value interface{}, ok bool)

func ContextPlaceholderLookupMap(m map[string]interface{}) ContextPlaceholderLookup {
	return func(key string) (value interface{}, ok bool) {
		v, ok := m[key]
		return v, ok
	}
}

func (c ContextPlaceholderLookup) Or(next ContextPlaceholderLookup) ContextPlaceholderLookup {
	return func(key string) (value interface{}, ok bool) {
		if v, ok := c(key); ok {
			return v, true
		}
		return next(key)
	}
}

type ProviderIndexEntry struct {
	platformorchestratorcp.ModuleProvider
	HasResourcePlaceholders bool
}

// BuildProviderIndex indexes a list of module providers into a map of type.id to ProviderIndexEntry
func BuildProviderIndex(providers []platformorchestratorcp.ModuleProvider) map[string]ProviderIndexEntry {
	index := make(map[string]ProviderIndexEntry, len(providers))
	for _, p := range providers {
		var hasResourcePlaceholders bool
		var ignored error
		for _, sub := range platform_orchestrator_graph.IterPlaceholders(p.Configuration, &ignored) {
			if sub.Type() == platform_orchestrator_graph.PlaceholderTypeResource {
				hasResourcePlaceholders = true
				break
			}
		}
		index[fmt.Sprintf("%s.%s", p.ProviderType, p.Id)] = ProviderIndexEntry{
			ModuleProvider:          p,
			HasResourcePlaceholders: hasResourcePlaceholders,
		}
	}
	return index
}

func LocalProviderName(providerType, providerId, variant string) string {
	return strings.ReplaceAll(fmt.Sprintf("%s-%s-%s", providerType, providerId, variant[:8]), "_", "")
}

func BuildTofuFromGraph(g *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], providers []platformorchestratorcp.ModuleProvider, runner platformorchestratorcp.Runner, envUuid uuid.UUID,
	contextPlaceholderLookup ContextPlaceholderLookup, matrix platform_orchestrator_graph.DistanceMatrix) ([]byte, error) {
	f := hclwrite.NewEmptyFile()

	provIndex := BuildProviderIndex(providers)
	configuredProviders := make(map[string]bool)
	configuredVars := make(map[string]bool)

	metadata := make([]hclwrite.ObjectAttrTokens, 0)

	tfBlock := f.Body().AppendNewBlock("terraform", []string{})
	if err := writeStateStorageBlock(runner, tfBlock, envUuid); err != nil {
		return nil, err
	}
	rpBlock := tfBlock.Body().AppendNewBlock("required_providers", []string{})

	for rc := range g.DepthFirstIterate(platform_orchestrator_graph.DepthFirstIteratePostOrder) {
		if rc.Type == platform_orchestrator_graph.DefaultWorkloadResourceType {
			// skip these for now - we'll get to them after the graph iteration
			continue
		}

		n := g.Nodes[rc]
		if n.ModuleConfiguration == nil || n.ModuleConfiguration.Definition == nil {
			return nil, fmt.Errorf("module configuration is nil on node %s (%v)", rc.String(), n)
		}

		resourceContextLookup := contextPlaceholderLookup.Or(ContextPlaceholderLookupMap(map[string]interface{}{
			"res_type":  rc.Type,
			"res_class": rc.Class,
			"res_id":    rc.Id,
		}))

		// We need to look up the provider mapping for all nodes including deleted ones, so we must do this before skipping
		// deleted nodes.
		var providerMappingBlock hclwrite.Tokens
		if pm := n.ModuleConfiguration.Definition.ProviderMapping; len(pm) > 0 {
			obj := make([]hclwrite.ObjectAttrTokens, 0, len(pm))
			for _, localRef := range slices.Sorted(maps.Keys(pm)) {
				pi, ok := provIndex[pm[localRef]]
				if !ok {
					return nil, UserBadRequestError(fmt.Sprintf(
						"unknown provider '%s' used by module definition %s@%s for %s", pm[localRef], n.ModuleConfiguration.DefinitionId, n.ModuleConfiguration.VersionId, rc.String(),
					))
				}

				// Look up by full provider identifier (pm[localRef])
				fullProviderIdentifier := pm[localRef]
				providerVariantHash, ok := n.ModuleConfiguration.ProviderFullIdToHashVariantMapping[fullProviderIdentifier]
				if !ok {
					return nil, fmt.Errorf("provider variant hash not found for provider '%s' with local ref '%s' on node '%s'", fullProviderIdentifier, localRef, rc.String())
				}
				subs := n.ModuleConfiguration.ProviderSubsMap[localRef]
				localProviderName := LocalProviderName(pi.ProviderType, pi.Id, providerVariantHash)

				if _, ok := configuredProviders[providerVariantHash]; !ok {
					objProps := make(map[string]cty.Value)
					objProps["source"] = cty.StringVal(pi.Source)
					objProps["version"] = cty.StringVal(pi.VersionConstraint)
					rpBlock.Body().SetAttributeValue(localProviderName, cty.ObjectVal(objProps))

					f.Body().AppendNewline()
					block := f.Body().AppendNewBlock("provider", []string{localProviderName})
					block.Body().SetAttributeValue("alias", cty.StringVal(localProviderName))

					if err := addProviderConfigurationMapToBlock(block, nil, pi.Configuration, subs, contextPlaceholderLookup); err != nil {
						return nil, err
					}

					configuredProviders[providerVariantHash] = true
				}

				for _, s := range subs {
					if vs, ok := s.(*platform_orchestrator_graph.TfVarPlaceholder); ok && !configuredVars[vs.Key] {
						f.Body().AppendNewline()
						block := f.Body().AppendNewBlock("variable", []string{vs.Key})
						block.Body().SetAttributeRaw("type", hclwrite.TokensForIdentifier("string"))
						block.Body().SetAttributeValue("sensitive", cty.BoolVal(true))
						configuredVars[vs.Key] = true
					}
				}

				obj = append(obj, hclwrite.ObjectAttrTokens{
					Name:  hclwrite.TokensForIdentifier(localRef),
					Value: hclwrite.TokensForIdentifier(localProviderName + "." + localProviderName),
				})
			}
			providerMappingBlock = hclwrite.TokensForObject(obj)
		}

		if n.ModuleConfiguration.Deleted {
			continue
		}

		tofuModuleName := toModuleName(rc)
		f.Body().AppendNewline()
		block := f.Body().AppendNewBlock("module", []string{tofuModuleName})

		moduleSource, moduleVersion := n.ModuleConfiguration.Definition.ModuleSource, ""
		moduleSourceCode := n.ModuleConfiguration.Definition.ModuleSourceCode
		if moduleSourceCode != nil {
			// if the source of the module is in our db, then we will generate a final output directory
			block.Body().SetAttributeValue("source", cty.StringVal(fmt.Sprintf("./modules/%s/%s", n.ModuleConfiguration.DefinitionId, n.ModuleConfiguration.VersionId)))
		} else if moduleSource != "" {
			moduleName := moduleSource
			// If this is a registry module, then we can support the @ as a version, but the other types cannot
			if i := strings.LastIndex(moduleName, "@"); i >= 0 && !strings.HasPrefix(moduleName, "/") && !strings.HasPrefix(moduleName, "git::") {
				moduleVersion = moduleName[i+1:]
				moduleName = moduleName[:i]
			}

			block.Body().SetAttributeValue("source", cty.StringVal(moduleName))
			if moduleVersion != "" {
				block.Body().SetAttributeValue("version", cty.StringVal(moduleVersion))
			}
		} else {
			return nil, fmt.Errorf("module must have source or inline code on node %s (%v)", rc.String(), n)
		}

		metadata = append(metadata, hclwrite.ObjectAttrTokens{
			Name:  hclwrite.TokensForValue(cty.StringVal(util.GenerateNodeHash(envUuid, rc.Type, rc.Class, rc.Id))),
			Value: hclwrite.TokensForFunctionCall("lookup", hclwrite.TokensForIdentifier(fmt.Sprintf("module.%s", tofuModuleName)), hclwrite.TokensForValue(cty.StringVal(platformOrchestratorMetadataOutput)), hclwrite.TokensForValue(cty.EmptyObjectVal)),
		})

		if len(providerMappingBlock) > 0 {
			block.Body().SetAttributeRaw("providers", providerMappingBlock)
		}

		seen := make(map[string]bool)
		if len(n.ModuleConfiguration.Definition.ModuleInputs) > 0 {
			block.Body().AppendNewline()
			for _, k := range slices.Sorted(maps.Keys(n.ModuleConfiguration.Definition.ModuleInputs)) {
				var err error
				if subs := maps.Collect(platform_orchestrator_graph.IterPlaceholders(n.ModuleConfiguration.Definition.ModuleInputs[k], &err)); err != nil {
					return nil, UserBadRequestError(fmt.Sprintf("node '%s': failed to parse placeholders for module input '%s': %v", rc, k, err))
				} else if err := resolveResourceSubstitutions(g, matrix, rc, rc, subs); err != nil {
					return nil, UserBadRequestError(fmt.Sprintf("node '%s': failed to resolve placeholders for module input '%s': %v", rc, k, err))
				} else if tokens, err := convertToTokens(n.ModuleConfiguration.Definition.ModuleInputs[k], subs, placeholdersSupportedInModule, resourceContextLookup); err != nil {
					return nil, UserBadRequestError(fmt.Sprintf("node '%s': failed to create HCL tokens for module input '%s': %v", rc, k, err))
				} else if len(tokens) > 0 {
					block.Body().SetAttributeRaw(k, tokens)
				}
				seen[k] = true
			}
		}

		if len(n.Params) > 0 {
			block.Body().AppendNewline()
			for _, k := range slices.Sorted(maps.Keys(n.Params)) {
				if _, ok := seen[k]; ok {
					continue
				}
				var err error
				if subs := maps.Collect(platform_orchestrator_graph.IterPlaceholders(n.Params[k], &err)); err != nil {
					return nil, UserBadRequestError(fmt.Sprintf("node '%s': failed to parse placeholders for param '%s': %v", rc, k, err))
				} else if err := resolveResourceSubstitutions(g, matrix, rc, ref.DerefOr(n.ParamsDefinedBy, rc), subs); err != nil {
					return nil, UserBadRequestError(fmt.Sprintf("node '%s': failed to resolve placeholders for param '%s': %v", rc, k, err))
				} else if tokens, err := convertToTokens(n.Params[k], subs, placeholdersSupportedInModule, resourceContextLookup); err != nil {
					return nil, UserBadRequestError(fmt.Sprintf("node '%s': failed to create HCL tokens for param '%s': %v", rc, k, err))
				} else if len(tokens) > 0 {
					block.Body().SetAttributeRaw(k, tokens)
				}
			}
		}

		if deps := g.Edges[rc]; len(deps) > 0 {
			block.Body().AppendNewline()
			tup := make([]hclwrite.Tokens, 0, len(deps))
			for _, key := range slices.SortedFunc(maps.Values(deps), platform_orchestrator_graph.CompareResourceCoordinate) {
				tup = append(tup, hclwrite.TokensForTraversal(hcl.Traversal{
					hcl.TraverseRoot{Name: hclModuleRoot},
					hcl.TraverseAttr{Name: toModuleName(key)},
				}))
			}
			block.Body().SetAttributeRaw("depends_on", hclwrite.TokensForTuple(tup))
		}
	}

	f.Body().AppendNewline()
	block := f.Body().AppendNewBlock("output", []string{platformOrchestratorMetadataOutput})
	block.Body().SetAttributeRaw("value", hclwrite.TokensForObject(metadata))
	block.Body().SetAttributeValue("description", cty.StringVal("The metadata output from the modules involved in the deployment"))

	for _, name := range slices.Sorted(maps.Keys(g.Workloads)) {
		rc := g.Workloads[name]
		n := g.Nodes[rc]

		attrTokens := make([]hclwrite.ObjectAttrTokens, 0, len(n.Params))
		for _, k := range slices.Sorted(maps.Keys(n.Params)) {
			v := n.Params[k]
			var err error
			if subs := maps.Collect(platform_orchestrator_graph.IterPlaceholders(v, &err)); err != nil {
				return nil, UserBadRequestError(fmt.Sprintf("node '%s': failed to parse placeholders for variable '%s': %v", rc, k, err))
			} else if err := resolveResourceSubstitutions(g, matrix, rc, rc, subs); err != nil {
				return nil, UserBadRequestError(fmt.Sprintf("node '%s': failed to resolve placeholders for variable '%s': %v", rc, k, err))
			} else if tokens, err := convertToTokens(v, subs, placeholdersSupportedInWorkload, contextPlaceholderLookup); err != nil {
				return nil, UserBadRequestError(fmt.Sprintf("failed to create HCL tokens for variable '%s': %v", k, err))
			} else if len(tokens) > 0 {
				attrTokens = append(attrTokens, hclwrite.ObjectAttrTokens{
					Name:  hclwrite.TokensForIdentifier(k),
					Value: tokens,
				})
			}
		}
		f.Body().AppendNewline()
		block := f.Body().AppendNewBlock("output", []string{name})
		block.Body().SetAttributeRaw("value", hclwrite.TokensForObject(attrTokens))
		block.Body().SetAttributeValue("description", cty.StringVal(fmt.Sprintf("The output variables for workload '%s'", name)))
		block.Body().SetAttributeValue("sensitive", cty.BoolVal(true))
	}

	return f.Bytes(), nil
}

// addProviderConfigurationMapToBlock adds the map attributes from data to the block. It supports the [N] suffix to denote sub blocks that are not plain attributes.
func addProviderConfigurationMapToBlock(block *hclwrite.Block, path []string, data map[string]interface{}, subs map[string]platform_orchestrator_graph.PlaceholderSub, contextPlaceholderLookup ContextPlaceholderLookup) error {
	for _, k := range slices.Sorted(maps.Keys(data)) {
		asMap, isMap := data[k].(map[string]interface{})
		if m := blockConfigurationSuffixRegex.FindString(k); m != "" && isMap {
			trimmedKey := k[:len(k)-len(m)+1]
			subBlock := block.Body().AppendNewBlock(trimmedKey, []string{})
			if err := addProviderConfigurationMapToBlock(subBlock, append(path, k), asMap, subs, contextPlaceholderLookup); err != nil {
				return err
			}
		} else {
			tokens, err := convertToTokens(data[k], subs, placeholdersSupportedInProvider, contextPlaceholderLookup)
			if err != nil {
				return errors.Wrapf(err, "failed to render provider configuration value for key '%s'", strings.Join(append(path, k), "."))
			}
			block.Body().SetAttributeRaw(k, tokens)
		}
	}
	return nil
}

// hashUniqueProviderConfiguration generates a hash key for each unique configuration of a provider type+id.
func hashUniqueProviderConfiguration(providerType, providerId string, subs map[string]platform_orchestrator_graph.PlaceholderSub) string {
	h := sha256.New()
	// Type and id MUST be part of the hash key, because we only count the variants to be equal if they are the same
	// configuration of the provider. For backward compatibility, we encode these raw without gob-encoding.
	_, _ = fmt.Fprint(h, providerType, providerId)

	// Go through the map keys in sorted order
	for _, k := range slices.Sorted(maps.Keys(subs)) {
		// Now we're going to encode ONLY the resource/selector placeholders that are dynamic. All other placeholders,
		// like context keys and TF_VAR expressions are automatically the same across all copies of the same provider.
		switch typed := subs[k].(type) {
		case *platform_orchestrator_graph.SelectorPlaceholder:
			// Encode the key - this contains the type, selector, and output keys
			_, _ = fmt.Fprint(h, len(k), k)
			_, _ = fmt.Fprint(h, len(typed.MatchedCoordinates))
			for _, c := range typed.MatchedCoordinates {
				rc := c.String()
				_, _ = fmt.Fprint(h, len(rc), rc)
			}
		case *platform_orchestrator_graph.ResourcePlaceholder:
			// Encode the key - this contains the type, alias, and output keys
			_, _ = fmt.Fprint(h, len(k), k)
			rc := typed.Coordinate.String()
			_, _ = fmt.Fprint(h, len(rc), rc)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeStateStorageBlock(r platformorchestratorcp.Runner, tfBlock *hclwrite.Block, deUuid uuid.UUID) error {
	storageType, _ := r.StateStorageConfiguration.Discriminator()
	switch storageType {
	case string(platformorchestratorcp.StateStorageTypeKubernetes):
		if b, err := r.StateStorageConfiguration.AsK8sStorageConfiguration(); err != nil {
			return errors.Wrap(err, "failed to convert state storage configuration to kubernetes backend")
		} else {
			k8sbeBlock := tfBlock.Body().AppendNewBlock("backend", []string{"kubernetes"})
			suffix := deUuid.String()
			k8sbeBlock.Body().SetAttributeValue("secret_suffix", cty.StringVal(suffix))
			k8sbeBlock.Body().SetAttributeValue("namespace", cty.StringVal(b.Namespace))
			// Critically, we want to use the service account of the k8s runner to store the secrets in cluster.
			// Setting this flag, should avoid us from picking up other environment variable settings.
			k8sbeBlock.Body().SetAttributeValue("in_cluster_config", cty.BoolVal(true))
		}
		return nil
	case string(platformorchestratorcp.StateStorageTypeS3):
		if b, err := r.StateStorageConfiguration.AsS3StorageConfiguration(); err != nil {
			return errors.Wrap(err, "failed to convert state storage configuration to s3 backend")
		} else {
			s3beBlock := tfBlock.Body().AppendNewBlock("backend", []string{"s3"})
			s3beBlock.Body().SetAttributeValue("bucket", cty.StringVal(b.Bucket))
			pp := fmt.Sprintf("%s.tfstate", deUuid)
			if b.PathPrefix != nil {
				pp = path.Join(*b.PathPrefix, pp)
			}
			s3beBlock.Body().SetAttributeValue("key", cty.StringVal(pp))
		}
		return nil
	case string(platformorchestratorcp.StateStorageTypeGcs):
		if b, err := r.StateStorageConfiguration.AsGCSStorageConfiguration(); err != nil {
			return errors.Wrap(err, "failed to convert state storage configuration to gcs backend")
		} else {
			gcsbeBlock := tfBlock.Body().AppendNewBlock("backend", []string{"gcs"})
			gcsbeBlock.Body().SetAttributeValue("bucket", cty.StringVal(b.Bucket))
			pp := deUuid.String()
			if b.PathPrefix != nil {
				pp = path.Join(*b.PathPrefix, pp)
			}
			gcsbeBlock.Body().SetAttributeValue("prefix", cty.StringVal(pp))
		}
		return nil
	case string(platformorchestratorcp.StateStorageTypeAzurerm):
		if b, err := r.StateStorageConfiguration.AsAzureRMStorageConfiguration(); err != nil {
			return errors.Wrap(err, "failed to convert state storage configuration to azurerm backend")
		} else {
			azurermbeBlock := tfBlock.Body().AppendNewBlock("backend", []string{"azurerm"})
			if b.ResourceGroupName != nil {
				azurermbeBlock.Body().SetAttributeValue("resource_group_name", cty.StringVal(*b.ResourceGroupName))
			}
			azurermbeBlock.Body().SetAttributeValue("storage_account_name", cty.StringVal(b.StorageAccountName))
			azurermbeBlock.Body().SetAttributeValue("container_name", cty.StringVal(b.ContainerName))
			pp := deUuid.String()
			if b.PathPrefix != nil {
				pp = path.Join(*b.PathPrefix, pp)
			}
			azurermbeBlock.Body().SetAttributeValue("key", cty.StringVal(path.Join(pp, "terraform.tfstate")))
		}
		return nil
	default:
		return UserBadRequestError(fmt.Sprintf("unsupported runner state storage type: %s", storageType))
	}
}

var stripIdentifierPattern = regexp.MustCompile(`[^a-zA-Z0-9_-]+`)

const maximumModuleNameLength = 100
const moduleNameHashLength = 8

// toModuleName returns a useful module name based on the type,class,id of the node and stripping any non-supported
// characters while also keeping it unique and length limited.
func toModuleName(key platform_orchestrator_graph.ResourceCoordinate) string {
	base := fmt.Sprintf("%s_%s_%s", key.Type, key.Class, key.Id)
	prefix := stripIdentifierPattern.ReplaceAllString(base, "")
	if l := maximumModuleNameLength - moduleNameHashLength - 1; len(prefix) > l {
		prefix = prefix[:l]
	}
	h := sha256.New()
	_, _ = fmt.Fprint(h, base)
	return prefix + "_" + hex.EncodeToString(h.Sum(nil))[:moduleNameHashLength]
}

func yamlValueToCtyValue(i interface{}) (cty.Value, error) {
	if i == nil {
		return cty.NilVal, nil
	} else {
		switch v := i.(type) {
		case string:
			return gocty.ToCtyValue(v, cty.String)
		case int:
			return gocty.ToCtyValue(v, cty.Number)
		case float32:
			return gocty.ToCtyValue(v, cty.Number)
		case float64:
			return gocty.ToCtyValue(v, cty.Number)
		case bool:
			return gocty.ToCtyValue(v, cty.Bool)
		case map[string]interface{}:
			return handleMap(v)
		case []interface{}:
			return handleSlice(v)
		default:
			return cty.NilVal, fmt.Errorf("unsupported type '%T'", v)
		}
	}
}

func handleMap(m map[string]interface{}) (cty.Value, error) {
	if len(m) == 0 {
		return cty.EmptyObjectVal, nil
	}
	ctyMap := make(map[string]cty.Value)
	for key, value := range m {
		convertedValue, err := yamlValueToCtyValue(value)
		if err != nil {
			return cty.NilVal, errors.Wrapf(err, "failed to convert value for key '%s'", key)
		}
		ctyMap[key] = convertedValue
	}

	return cty.ObjectVal(ctyMap), nil
}

func handleSlice(s []interface{}) (cty.Value, error) {
	if len(s) == 0 {
		return cty.ListValEmpty(cty.DynamicPseudoType), nil
	}
	elTypes := make([]cty.Type, len(s))
	for idx, v := range s {
		if elType, err := gocty.ImpliedType(v); err != nil {
			return cty.NilVal, errors.Wrapf(err, "failed to obtain type for element of list with idx '%d'", idx)
		} else {
			elTypes[idx] = elType
		}
	}

	return gocty.ToCtyValue(s, cty.Tuple(elTypes))
}

// placeholderToTokens returns the appropriate HCL tokens for the placeholder expression provided. If the placeholder
// expression is not supported or a substitution was not previously resolved, an error is returned. This is called by
// convertToTokens below.
func placeholderToTokens(value string, subs map[string]platform_orchestrator_graph.PlaceholderSub, supportedTypes []platform_orchestrator_graph.PlaceholderType, contextPlaceholderLookup ContextPlaceholderLookup) (tokens hclwrite.Tokens, err error) {
	sub, ok := subs[value]
	if ok {
		if !slices.Contains(supportedTypes, sub.Type()) {
			return nil, errors.Errorf("'%s' placeholders are not supported here", sub.Type())
		}
		switch typed := sub.(type) {
		case *platform_orchestrator_graph.SelectorPlaceholder:
			tuple := make([]hclwrite.Tokens, 0, len(typed.MatchedCoordinates))
			for _, c := range typed.MatchedCoordinates {
				traversal := hcl.Traversal{
					hcl.TraverseRoot{Name: hclModuleRoot},
					hcl.TraverseAttr{Name: toModuleName(c)},
				}
				for _, k := range typed.Output {
					traversal = append(traversal, hcl.TraverseAttr{Name: k})
				}
				tuple = append(tuple, hclwrite.TokensForTraversal(traversal))
			}
			return hclwrite.TokensForTuple(tuple), nil
		case *platform_orchestrator_graph.ResourcePlaceholder:
			if typed.Coordinate != nil {
				traversal := hcl.Traversal{
					hcl.TraverseRoot{Name: hclModuleRoot},
					hcl.TraverseAttr{Name: toModuleName(*typed.Coordinate)},
				}
				for _, k := range typed.Output {
					traversal = append(traversal, hcl.TraverseAttr{Name: k})
				}
				return hclwrite.TokensForTraversal(traversal), nil
			}
		case *platform_orchestrator_graph.TfVarPlaceholder:
			return hclwrite.TokensForTraversal(hcl.Traversal{hcl.TraverseRoot{Name: "var"}, hcl.TraverseAttr{Name: typed.Key}}), nil
		case *platform_orchestrator_graph.ContextPlaceholder:
			if v, ok := contextPlaceholderLookup(typed.Key); ok {
				vv, _ := yamlValueToCtyValue(v)
				return hclwrite.TokensForValue(vv), nil
			}
			return nil, errors.Errorf("unsupported context key '%s'", typed.Key)
		default:
		}
	}
	return nil, errors.Wrapf(err, "substitution for placeholder %s not found", value)
}

// convertToTokens resolves all placeholders within an arbitrary json-like string, object, or array.
func convertToTokens(i interface{}, subs map[string]platform_orchestrator_graph.PlaceholderSub, supportedTypes []platform_orchestrator_graph.PlaceholderType, contextPlaceholderLookup ContextPlaceholderLookup) (tokens hclwrite.Tokens, err error) {
	if i == nil {
		return
	}
	switch v := i.(type) {
	case string:
		matches := platform_orchestrator_graph.RePlaceholder.FindAllStringSubmatch(v, -1)
		if len(matches) == 0 {
			unescaped := strings.ReplaceAll(v, `$\{`, `${`)
			if convertedValue, err := yamlValueToCtyValue(unescaped); err != nil {
				return nil, err
			} else {
				return hclwrite.TokensForValue(convertedValue), nil
			}
		}
		if len(matches) == 1 && matches[0][0] == v {
			// The entire value is a placeholder, convert to a simple expression without interpolation
			return placeholderToTokens(v, subs, supportedTypes, contextPlaceholderLookup)
		}
		// Create an interpolated string
		tokens = append(tokens, &hclwrite.Token{
			Type:  hclsyntax.TokenOQuote,
			Bytes: []byte{'"'},
		})

		lastPos := 0
		for _, match := range matches {
			placeholder := match[0]
			start := strings.Index(v[lastPos:], placeholder) + lastPos

			if start > lastPos {
				literalText := strings.ReplaceAll(v[lastPos:start], `$\{`, `${`)
				// take advantage of TF string normalization but skip the prefix and suffix quotes.
				t := hclwrite.TokensForValue(cty.StringVal(literalText))
				tokens = append(tokens, t[1:len(t)-1]...)
			}

			placeholderTokens, err := placeholderToTokens(placeholder, subs, supportedTypes, contextPlaceholderLookup)
			if err != nil {
				return nil, err
			}

			tokens = append(tokens, &hclwrite.Token{
				Type:  hclsyntax.TokenTemplateInterp,
				Bytes: []byte("${"),
			})
			tokens = append(tokens, placeholderTokens...)
			tokens = append(tokens, &hclwrite.Token{
				Type:  hclsyntax.TokenTemplateSeqEnd,
				Bytes: []byte("}"),
			})

			lastPos = start + len(placeholder)
		}

		if lastPos < len(v) {
			literalText := strings.ReplaceAll(v[lastPos:], `$\{`, `${`)
			// take advantage of TF string normalization but skip the prefix and suffix quotes.
			t := hclwrite.TokensForValue(cty.StringVal(literalText))
			tokens = append(tokens, t[1:len(t)-1]...)
		}

		tokens = append(tokens, &hclwrite.Token{
			Type:  hclsyntax.TokenCQuote,
			Bytes: []byte{'"'},
		})
	case map[string]interface{}:
		tokens = append(tokens, &hclwrite.Token{
			Type:  hclsyntax.TokenOBrace,
			Bytes: []byte("{"),
		})
		tokens = append(tokens, &hclwrite.Token{
			Type:  hclsyntax.TokenNewline,
			Bytes: []byte("\n"),
		})
		for _, key := range slices.Sorted(maps.Keys(v)) {
			valueTokens, err := convertToTokens(v[key], subs, supportedTypes, contextPlaceholderLookup)
			if err != nil {
				return nil, err
			}
			if hclsyntax.ValidIdentifier(key) {
				tokens = append(tokens, hclwrite.TokensForIdentifier(key)...)
			} else {
				tokens = append(tokens, hclwrite.TokensForValue(cty.StringVal(key))...)
			}
			tokens = append(tokens, &hclwrite.Token{
				Type:  hclsyntax.TokenEqual,
				Bytes: []byte("="),
			})
			tokens = append(tokens, valueTokens...)
			tokens = append(tokens, &hclwrite.Token{
				Type:  hclsyntax.TokenNewline,
				Bytes: []byte("\n"),
			})
		}
		tokens = append(tokens, &hclwrite.Token{
			Type:  hclsyntax.TokenCBrace,
			Bytes: []byte("}"),
		})
	case []interface{}:
		tokens = append(tokens, &hclwrite.Token{
			Type:  hclsyntax.TokenOBrack,
			Bytes: []byte("["),
		})
		tokens = append(tokens, &hclwrite.Token{
			Type:  hclsyntax.TokenNewline,
			Bytes: []byte("\n"),
		})
		for ind, value := range v {
			valueTokens, err := convertToTokens(value, subs, supportedTypes, contextPlaceholderLookup)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, valueTokens...)
			if ind+1 < len(v) {
				tokens = append(tokens, &hclwrite.Token{
					Type:  hclsyntax.TokenComma,
					Bytes: []byte(","),
				})
			}
			tokens = append(tokens, &hclwrite.Token{
				Type:  hclsyntax.TokenNewline,
				Bytes: []byte("\n"),
			})
		}
		tokens = append(tokens, &hclwrite.Token{
			Type:  hclsyntax.TokenCBrack,
			Bytes: []byte("]"),
		})
	default:
		if convertedValue, err := yamlValueToCtyValue(v); err != nil {
			return nil, err
		} else {
			tokens = hclwrite.TokensForValue(convertedValue)
		}
	}
	return
}

// fillInResourceParamDependencies does best effort first-pass to set resource placeholders as real dependencies.
// this skips any errors since we do a more detailed parsing and resolving later when we generate the Tofu.
func fillInResourceParamDependencies(g *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], matrix platform_orchestrator_graph.DistanceMatrix) {
	for rc, n := range g.Nodes {
		var ignored error
		if n.Params != nil {
			for _, sub := range platform_orchestrator_graph.IterPlaceholders(n.Params, &ignored) {
				if resSub, ok := sub.(*platform_orchestrator_graph.ResourcePlaceholder); ok {
					if g.ResolveResourcePlaceholder(ref.DerefOr(n.ParamsDefinedBy, rc), resSub) == nil {
						if existing, ok := matrix[rc]; ok {
							existing[*resSub.Coordinate] = 1
							matrix[rc] = existing
						} else {
							matrix[rc] = map[platform_orchestrator_graph.ResourceCoordinate]int{*resSub.Coordinate: 1}
						}
					}
				}
			}
		}
	}
}

func resolveResourceSubstitutions(g *platform_orchestrator_graph.Graph[*GraphNodeModuleConfig], matrix platform_orchestrator_graph.DistanceMatrix, coordinate platform_orchestrator_graph.ResourceCoordinate,
	contextCoordinate platform_orchestrator_graph.ResourceCoordinate, subs map[string]platform_orchestrator_graph.PlaceholderSub) error {
	for exp, pc := range subs {
		if rsub, ok := pc.(*platform_orchestrator_graph.ResourcePlaceholder); ok {
			if err := g.ResolveResourcePlaceholder(contextCoordinate, rsub); err != nil {
				return errors.Wrapf(err, "invalid placeholder '%s'", exp)
			}
			// if this resolves to a deleted node, return an error!
			if n, ok := g.Nodes[*rsub.Coordinate]; ok && n.ModuleConfiguration != nil && n.ModuleConfiguration.Deleted {
				return fmt.Errorf("placeholder '%s' resolves to a deleted node '%s' which must be anchored in the graph", exp, *rsub.Coordinate)
			}
			_ = g.AddAnonymousDependency(coordinate, *rsub.Coordinate)
			subs[exp] = pc
		} else if ssub, ok := pc.(*platform_orchestrator_graph.SelectorPlaceholder); ok {
			if err := matrix.ResolveSelectorPlaceholder(contextCoordinate, ssub); err != nil {
				return errors.Wrapf(err, "invalid placeholder '%s'", exp)
			}
			for _, mc := range ssub.MatchedCoordinates {
				// skip deleted nodes just in case we're still picking them up here
				if n, ok := g.Nodes[mc]; ok && n.ModuleConfiguration != nil && n.ModuleConfiguration.Deleted {
					continue
				}
				_ = g.AddAnonymousDependency(coordinate, mc)
			}
			subs[exp] = pc
		}
		// NOTE: any other placeholder types don't need graph resolution
	}
	return nil
}
