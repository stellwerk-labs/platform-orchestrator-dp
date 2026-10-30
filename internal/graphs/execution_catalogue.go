package graphs

import (
	"encoding/json"
	"fmt"

	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
)

// DecodeExecutionCatalogue accepts the established published wire format while
// keeping the richer executable type separate from the existing SDK contract.
func DecodeExecutionCatalogue(raw []byte, published *cp.InternalModuleCatalogue) (*cp.InternalExecutionCatalogue, error) {
	if len(raw) == 0 {
		if published == nil {
			return nil, fmt.Errorf("module catalogue response is missing")
		}
		var err error
		raw, err = json.Marshal(published)
		if err != nil {
			return nil, err
		}
	}
	var result cp.InternalExecutionCatalogue
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
