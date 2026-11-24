package nativelogs

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/stellwerk-labs/golib/hmessaging"
)

const ReadyEventType = "log-object-ready"
const Bucket = "PO_RUNNER_LOGS"
const DescriptorHeader = "X-Stellwerk-Native-Log"

const (
	readyHeaderCountLimit = 64
	readyHeaderNameLimit  = 128
	readyHeaderValueLimit = 4096
)

// ParseDescriptor accepts only the exact metadata schema. JSON's usual
// case-insensitive struct matching must not introduce authority aliases.
func ParseCapture(data []byte) (Descriptor, error) {
	var d Descriptor
	if StrictJSON(data, &d, MetadataLimit) != nil || d.Validate() != nil ||
		!exactMembers(data, "version authority status ciphertext_sha256 ciphertext_bytes object_key") {
		return Descriptor{}, ErrArtifact
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	if !exactMembers(fields["authority"], "organization_id environment_id deployment_id command_digest target phase step_id execution_id capture_revision subject source_plan_id source_subject inherited") {
		return Descriptor{}, ErrArtifact
	}
	var authority map[string]json.RawMessage
	_ = json.Unmarshal(fields["authority"], &authority)
	if !exactMembers(authority["target"], "pipeline_version runner_id runner_uuid runtime_version state_backend_type state_configuration_digest tools_digest") {
		return Descriptor{}, ErrArtifact
	}
	for _, value := range []string{d.Authority.EnvironmentID, d.Authority.DeploymentID, d.Authority.Target.RunnerUUID} {
		id, err := uuid.Parse(value)
		if err != nil || id == uuid.Nil || id.String() != value {
			return Descriptor{}, ErrArtifact
		}
	}
	return d, nil
}

func exactMembers(data []byte, allowed string) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || len(fields) == 0 {
		return false
	}
	for name, value := range fields {
		if !slices.Contains(strings.Fields(allowed), name) || bytes.Equal(value, []byte("null")) {
			return false
		}
	}
	return true
}

// ParseReady binds the complete metadata envelope to its captured UUID route.
// It deliberately says nothing about unrelated Native event types.
func ParseReady(data []byte) (hmessaging.EventEnvelope, Descriptor, error) {
	var event hmessaging.EventEnvelope
	if StrictJSON(data, &event, ReadyEventLimit) != nil || !exactMembers(data, "protocol_version event_id command_id organization_id runner_id deployment_id type created_at payload") {
		return event, Descriptor{}, ErrArtifact
	}
	d, err := ParseDescriptor(event.Payload)
	route, routeErr := hmessaging.NativeRunnerToken(d.Authority.Target.RunnerUUID)
	if err != nil || routeErr != nil || event.ProtocolVersion != hmessaging.ProtocolVersionV1 || event.Type != ReadyEventType ||
		event.OrganizationID != d.Authority.OrganizationID || event.RunnerID != route || event.DeploymentID != d.Authority.DeploymentID ||
		event.CommandID != "" || event.EventID != d.EventID() || event.CreatedAt.IsZero() {
		return event, Descriptor{}, ErrArtifact
	}
	return event, d, nil
}

// VerifyReadyMessage additionally binds the carrier's subject and stream-wide ID.
func VerifyReadyMessage(message hmessaging.Message) (Descriptor, error) {
	event, d, err := ParseReady(message.Data)
	if err != nil {
		return Descriptor{}, err
	}
	subject, err := hmessaging.RunnerEventSubject(event.OrganizationID, event.RunnerID, event.Type)
	id, idErr := hmessaging.NativeMessageID(event.OrganizationID, event.RunnerID, "events", event.Type, event.EventID)
	if err != nil || idErr != nil || message.Subject != subject || message.ID != id || !message.CreatedAt.Equal(event.CreatedAt) || !message.ExpiresAt.IsZero() {
		return Descriptor{}, ErrArtifact
	}
	if !validReadyHeaders(message.Header, id) {
		return Descriptor{}, ErrArtifact
	}
	return d, nil
}

func validReadyHeaders(header hmessaging.Header, id string) bool {
	if len(header) > readyHeaderCountLimit {
		return false
	}
	size := 0
	for name, values := range header {
		if len(name) > readyHeaderNameLimit {
			return false
		}
		size += len(name)
		for _, value := range values {
			if len(value) > readyHeaderValueLimit {
				return false
			}
			size += len(value)
		}
		if strings.EqualFold(name, hmessaging.HeaderMessageID) && (name != hmessaging.HeaderMessageID || len(values) != 1 || values[0] != id) {
			return false
		}
	}
	return size <= MetadataLimit
}

func ParseDescriptor(data []byte) (Descriptor, error) {
	d, err := ParseCapture(data)
	if err != nil || d.Status != PendingTransfer {
		return Descriptor{}, ErrArtifact
	}
	return d, nil
}
