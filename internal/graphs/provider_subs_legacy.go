package graphs

import (
	"encoding/gob"
	"fmt"

	graph "github.com/stellwerk-labs/platform-orchestrator-graph"
)

// Distinct types let gob read the historical concrete names without changing
// the names used to encode current graph types.
type legacyContextPlaceholder graph.ContextPlaceholder
type legacyTfVarPlaceholder graph.TfVarPlaceholder
type legacyResourcePlaceholder graph.ResourcePlaceholder
type legacySelectorPlaceholder graph.SelectorPlaceholder
type legacyResourceCoordinate graph.ResourceCoordinate

func (*legacyContextPlaceholder) Type() graph.PlaceholderType  { return graph.PlaceholderTypeContext }
func (*legacyTfVarPlaceholder) Type() graph.PlaceholderType    { return graph.PlaceholderTypeTfVar }
func (*legacyResourcePlaceholder) Type() graph.PlaceholderType { return graph.PlaceholderTypeResource }
func (*legacySelectorPlaceholder) Type() graph.PlaceholderType { return graph.PlaceholderTypeSelector }

func init() {
	gob.RegisterName("*canyon_graph.ContextPlaceholder", &legacyContextPlaceholder{})
	gob.RegisterName("*canyon_graph.TfVarPlaceholder", &legacyTfVarPlaceholder{})
	gob.RegisterName("*canyon_graph.ResourcePlaceholder", &legacyResourcePlaceholder{})
	gob.RegisterName("*canyon_graph.SelectorPlaceholder", &legacySelectorPlaceholder{})
	gob.RegisterName("*canyon_graph.ResourceCoordinate", &legacyResourceCoordinate{})
}

func normalizeProviderSubsMap(subs map[string]map[string]graph.PlaceholderSub) (map[string]map[string]graph.PlaceholderSub, error) {
	for _, values := range subs {
		for key, value := range values {
			switch v := value.(type) {
			case *legacyContextPlaceholder:
				values[key] = (*graph.ContextPlaceholder)(v)
			case *legacyTfVarPlaceholder:
				values[key] = (*graph.TfVarPlaceholder)(v)
			case *legacyResourcePlaceholder:
				values[key] = (*graph.ResourcePlaceholder)(v)
			case *legacySelectorPlaceholder:
				values[key] = (*graph.SelectorPlaceholder)(v)
			case *graph.ContextPlaceholder, *graph.TfVarPlaceholder, *graph.ResourcePlaceholder, *graph.SelectorPlaceholder, nil:
			default:
				return nil, fmt.Errorf("unsupported ProviderSubsMap placeholder type %T", value)
			}
		}
	}
	return subs, nil
}
