package graphs

import (
	"encoding/json"
	"io"

	po_graph "github.com/stellwerk-labs/platform-orchestrator-graph"
)

func FromJson(r io.Reader) (*po_graph.Graph[*GraphNodeModuleConfig], error) {
	var out *po_graph.Graph[*GraphNodeModuleConfig]
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func ToJson(g *po_graph.Graph[*GraphNodeModuleConfig]) ([]byte, error) {
	return json.Marshal(g)
}
