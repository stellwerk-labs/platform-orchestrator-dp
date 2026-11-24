package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"
)

// PipelinePostRepairAuthority is public metadata, never the signed control or
// private task material. Runner acceptance owns execution authority.
type PipelinePostRepairAuthority struct {
	ControlID string    `json:"control_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (a *PipelinePostRepairAuthority) UnmarshalJSON(raw []byte) error {
	invalid := errors.New("invalid post repair metadata")
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return invalid
	}
	var value PipelinePostRepairAuthority
	seen := make(map[string]bool)
	for decoder.More() {
		token, err = decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[name] {
			return invalid
		}
		seen[name] = true
		switch name {
		case "control_id":
			err = decoder.Decode(&value.ControlID)
		case "expires_at":
			err = decoder.Decode(&value.ExpiresAt)
		default:
			return invalid
		}
		if err != nil {
			return invalid
		}
	}
	id, idErr := uuid.Parse(value.ControlID)
	if _, err = decoder.Token(); err != nil || len(seen) != 2 || decoder.Decode(new(any)) != io.EOF || idErr != nil || id == uuid.Nil || id.String() != value.ControlID || value.ExpiresAt.IsZero() {
		return invalid
	}
	*a = value
	return nil
}
