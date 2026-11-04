// This independent encoder intentionally imports no production graph/DP package.
package main

import (
	"encoding/gob"
	old "fixture-encoder/canyon_graph"
	current "fixture-encoder/orchestrator_graph"
	"os"
)

type substitution struct {
	Key   string
	Value old.PlaceholderSub
}
type entry struct {
	LocalRef      string
	Substitutions []substitution
}
type canonical struct {
	Version int
	Entries []entry
}
type unknown struct{ Key string }

func (*unknown) Type() old.PlaceholderType { return "unknown" }

func write(name string, value any) {
	f, err := os.Create("../" + name + ".gob")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := gob.NewEncoder(f).Encode(value); err != nil {
		panic(err)
	}
}
func main() {
	gob.Register(&old.ContextPlaceholder{})
	gob.Register(&old.TfVarPlaceholder{})
	gob.Register(&old.ResourcePlaceholder{})
	gob.Register(&old.SelectorPlaceholder{})
	gob.Register(&old.ResourceCoordinate{})
	gob.Register(&current.ContextPlaceholder{})
	gob.Register(&current.TfVarPlaceholder{})
	gob.Register(&current.ResourcePlaceholder{})
	gob.Register(&current.SelectorPlaceholder{})
	gob.Register(&current.ResourceCoordinate{})
	gob.Register(&unknown{})
	if len(os.Args) == 2 && os.Args[1] == "integration" {
		write("historical-tfvar-integration", map[string]map[string]old.PlaceholderSub{"faulty": {"${var.legacy_provider_flag}": &old.TfVarPlaceholder{Key: "legacy_provider_flag"}}})
		return
	}
	coord := old.ResourceCoordinate{Type: "fixture-resource", Class: "fixture-class", Id: "fixture-id"}
	values := map[string]old.PlaceholderSub{
		"context":  &old.ContextPlaceholder{Key: "fixture-context"},
		"tfvar":    &old.TfVarPlaceholder{Key: "fixture-variable"},
		"resource": &old.ResourcePlaceholder{Coordinate: &coord, Alias: "fixture-alias", IsShared: true, IsSelf: true, Output: []string{"nested", "output"}},
		"selector": &old.SelectorPlaceholder{MatchedCoordinates: []old.ResourceCoordinate{coord, {Type: "second-type", Class: "second-class", Id: "second-id"}}, Functions: []string{"first", "last"}, Args: []string{"one", "two"}, Output: []string{"selector", "value"}},
	}
	write("historical-map", map[string]map[string]old.PlaceholderSub{"fixture-provider": values, "empty": {}})
	write("historical-canonical", canonical{Version: 1, Entries: []entry{{LocalRef: "fixture-provider", Substitutions: []substitution{{"context", values["context"]}, {"tfvar", values["tfvar"]}, {"resource", values["resource"]}, {"selector", values["selector"]}}}, {LocalRef: "empty"}}})
	mixed := map[string]map[string]old.PlaceholderSub{"fixture-provider": values}
	values["current"] = &current.TfVarPlaceholder{Key: "current-variable"}
	write("mixed-map", mixed)
	write("mixed-canonical", canonical{Version: 1, Entries: []entry{{LocalRef: "fixture-provider", Substitutions: []substitution{{"resource", values["resource"]}, {"current", values["current"]}}}}})
	write("unsupported-version", canonical{Version: 99})
	write("unknown-type", map[string]map[string]old.PlaceholderSub{"fixture-provider": {"unknown": &unknown{Key: "synthetic"}}})
}
