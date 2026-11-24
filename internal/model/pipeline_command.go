package model

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/stellwerk-labs/golib/hmessaging"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
)

var nativeCommandRunnerID = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,127}$`)

type frozenPipelineScope struct {
	ExecutionLimits *PipelineExecutionLimits           `json:"execution_limits"`
	NativeTarget    *cp.NativeExecutionTargetReference `json:"native_target"`
	Version         string                             `json:"version"`
	ID              string                             `json:"id"`
	OrgID           string                             `json:"org_id"`
	RunnerID        string                             `json:"runner_id"`
	RunnerUUID      string                             `json:"runner_uuid"`
	EnvironmentID   string                             `json:"environment_id"`
	ToolsDigest     string                             `json:"tools_digest"`
	IssuedAt        time.Time                          `json:"issued_at"`
	ExpiresAt       time.Time                          `json:"expires_at"`
	Request         struct {
		ID            string `json:"id"`
		OrgID         string `json:"org_id"`
		RunnerID      string `json:"runner_id"`
		RunnerUUID    string `json:"runner_uuid"`
		EnvironmentID string `json:"environment_id"`
		Backend       string `json:"backend"`
		Tool          string `json:"tool"`
		MetadataKey   string `json:"metadata_key"`
		Mode          string `json:"mode"`
		Recipient     string `json:"encrypted_outputs_recipient"`
		LogsRecipient string `json:"encrypted_logs_recipient"`
	} `json:"request"`
}

// FrozenPipelinePayload validates retained authority without consulting current
// Runner policy or re-encoding the original signed bytes used by effect receipts.
func FrozenPipelinePayload(raw []byte, dep *DeploymentSummary) (json.RawMessage, error) {
	invalid := errors.New("frozen pipeline command authority unavailable")
	var envelope hmessaging.CommandEnvelope
	var signed struct {
		Payload   json.RawMessage `json:"payload"`
		Signature string          `json:"signature"`
	}
	if dep == nil || len(raw) == 0 || len(raw) > 4<<20 || json.Unmarshal(raw, &envelope) != nil || json.Unmarshal(envelope.Payload, &signed) != nil || len(signed.Payload) == 0 {
		return nil, invalid
	}
	signature, err := base64.StdEncoding.DecodeString(signed.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize {
		return nil, invalid
	}
	if dep.NativeTarget != nil && !nativeCommandFields(signed.Payload) {
		return nil, invalid
	}
	var command frozenPipelineScope
	if json.Unmarshal(signed.Payload, &command) != nil {
		return nil, invalid
	}
	route := dep.RunnerId
	if dep.NativeTarget == nil {
		var extra struct {
			Target json.RawMessage `json:"native_target"`
			Limits json.RawMessage `json:"execution_limits"`
		}
		if json.Unmarshal(signed.Payload, &extra) != nil || len(extra.Target) != 0 || len(extra.Limits) != 0 {
			return nil, invalid
		}
		if command.Version != PipelineProtocol || command.NativeTarget != nil || command.RunnerUUID != "" || command.Request.RunnerUUID != "" {
			return nil, invalid
		}
	} else {
		if command.ExecutionLimits == nil || !command.ExecutionLimits.Valid() {
			return nil, invalid
		}
		target := dep.NativeTarget
		route, err = hmessaging.NativeRunnerToken(target.RunnerUuid.String())
		if err != nil || !validNativeExecutionTarget(target, dep.RunnerId) ||
			command.NativeTarget == nil ||
			*command.NativeTarget != *target ||
			command.Version != string(target.PipelineVersion) ||
			command.RunnerID != target.RunnerId ||
			command.RunnerUUID != target.RunnerUuid.String() ||
			command.Request.RunnerUUID != command.RunnerUUID ||
			command.ToolsDigest != target.ToolsDigest ||
			command.Request.Backend != string(target.StateBackendType) ||
			command.Request.MetadataKey != "" ||
			command.Request.Tool != "opentofu" {
			return nil, invalid
		}
	}
	if command.IssuedAt.IsZero() || !command.ExpiresAt.After(command.IssuedAt) || command.ExpiresAt.Sub(command.IssuedAt) > 7*24*time.Hour {
		return nil, invalid
	}
	if envelope.ProtocolVersion != hmessaging.ProtocolVersionV1 || envelope.Type != "execute-rollout-v1" || envelope.CommandID != "pipeline:"+dep.Id.String() || envelope.OrganizationID != dep.OrgId || envelope.RunnerID != route || envelope.DeploymentID != dep.Id.String() ||
		!envelope.CreatedAt.Equal(command.IssuedAt) ||
		!envelope.ExpiresAt.Equal(command.ExpiresAt) {
		return nil, invalid
	}
	mode := "deploy"
	if dep.Mode == DeploymentModeObservation {
		mode = "observation"
	} else if IsPlanDeployment(dep.Mode) {
		mode = "plan_only"
	} else {
		switch dep.Mode {
		case DeploymentModeRollback:
			mode = "rollback"
		case DeploymentModeDestroy:
			mode = "destroy"
		}
	}
	recipient := dep.EncryptedOutputsRecipient.Or("")
	if dep.Mode == DeploymentModeObservation {
		recipient = ""
	}
	if command.Request.Mode != mode || command.Request.Recipient != recipient || command.Request.LogsRecipient != "" && (dep.NativeTarget == nil || dep.Mode == DeploymentModeObservation || command.Request.LogsRecipient != dep.EncryptedLogsRecipient.Or("")) {
		return nil, invalid
	}
	if command.ID != dep.Id.String() || command.OrgID != dep.OrgId || command.RunnerID != dep.RunnerId || command.EnvironmentID != dep.DeploymentEnvUuid.String() || command.Request.ID != command.ID || command.Request.OrgID != command.OrgID || command.Request.RunnerID != command.RunnerID ||
		command.Request.EnvironmentID != command.EnvironmentID {
		return nil, invalid
	}
	return signed.Payload, nil
}

func nativeCommandDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Read names in order: projecting a map first would hide an earlier request.
func nativeCommandFields(payload []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	found, limits := false, false
	for decoder.More() {
		token, err = decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok {
			return false
		}
		if strings.EqualFold(name, "request") {
			if found || name != "request" || !readNativeRequest(decoder) {
				return false
			}
			found = true
		} else if strings.EqualFold(name, "execution_limits") {
			var value PipelineExecutionLimits
			if limits || name != "execution_limits" || decoder.Decode(&value) != nil {
				return false
			}
			limits = true
		} else {
			var value json.RawMessage
			if decoder.Decode(&value) != nil {
				return false
			}
		}
	}
	_, err = decoder.Token()
	return err == nil && found && limits && decoder.Decode(new(any)) == io.EOF
}

func readNativeRequest(decoder *json.Decoder) bool {
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := map[string]bool{}
	for decoder.More() {
		token, err = decoder.Token()
		name, ok := token.(string)
		if err != nil || !ok || seen[strings.ToLower(name)] || !slices.Contains(strings.Fields("id org_id runner_id runner_uuid environment_id bundle bundle_sha256 mode backend tool manifest encrypted_outputs_recipient encrypted_logs_recipient metadata_key"), name) {
			return false
		}
		seen[strings.ToLower(name)] = true
		var value json.RawMessage
		if decoder.Decode(&value) != nil || bytes.Equal(value, []byte("null")) {
			return false
		}
	}
	_, err = decoder.Token()
	return err == nil
}
