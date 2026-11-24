package api

import (
	"encoding/json"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
)

func validateNativeJobConfiguration(configuration any) error {
	raw, err := json.Marshal(configuration)
	if err != nil {
		return err
	}
	var config struct {
		Job struct {
			Environment map[string]string `json:"environment"`
			Secrets     map[string]string `json:"secrets"`
			PodTemplate map[string]any    `json:"pod_template"`
		} `json:"job"`
	}
	if err = json.Unmarshal(raw, &config); err != nil {
		return err
	}
	if len(config.Job.Environment)+len(config.Job.Secrets)+len(config.Job.PodTemplate) > 0 {
		return model.NewErrBadRequest("native Runner does not support job template or credential injection")
	}
	return nil
}
