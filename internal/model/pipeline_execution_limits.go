package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// PipelineExecutionLimits are frozen per-attempt ceilings, not a new lifetime.
type PipelineExecutionLimits struct {
	PlanSeconds  int64 `json:"plan_seconds"`
	ApplySeconds int64 `json:"apply_seconds"`
	TaskSeconds  int64 `json:"task_seconds"`
}

func (v PipelineExecutionLimits) Valid() bool {
	return v.PlanSeconds > 0 && v.PlanSeconds <= 14400 &&
		v.ApplySeconds > 0 && v.ApplySeconds <= 86400 &&
		v.TaskSeconds > 0 && v.TaskSeconds <= 14400
}

func (v *PipelineExecutionLimits) UnmarshalJSON(raw []byte) error {
	invalid := errors.New("execution limits unavailable")
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return invalid
	}
	var limits PipelineExecutionLimits
	fields := map[string]*int64{"plan_seconds": &limits.PlanSeconds, "apply_seconds": &limits.ApplySeconds, "task_seconds": &limits.TaskSeconds}
	seen := make(map[string]bool, len(fields))
	for decoder.More() {
		token, err = decoder.Token()
		name, ok := token.(string)
		field := fields[name]
		if err != nil || !ok || field == nil || seen[name] || decoder.Decode(field) != nil {
			return invalid
		}
		seen[name] = true
	}
	if _, err = decoder.Token(); err != nil || len(seen) != len(fields) || decoder.Decode(new(any)) != io.EOF || !limits.Valid() {
		return invalid
	}
	*v = limits
	return nil
}
