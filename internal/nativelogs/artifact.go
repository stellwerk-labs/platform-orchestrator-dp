// Package nativelogs defines the bounded Native phase-log wire artifact. It has
// no transport or key-custody policy; callers supply retained command authority.
package nativelogs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strconv"
	"strings"

	"filippo.io/age"
)

const (
	RawLimit            = 4 << 20
	EnvelopeLimit       = 6 << 20
	MetadataLimit       = 16 << 10
	CiphertextLimit     = EnvelopeLimit + ciphertextOverheadLimit
	DeliveryLimit       = 13 << 20
	ReadyEventLimit     = 32 << 10
	ReferenceLimit      = 128
	ReferenceBytesLimit = 128 << 10
	PlanPhase           = "plan"
	ApplyPhase          = "apply"
	PendingTransfer     = "pending_transfer"
	NotManaged          = "log_export_not_managed"
	Unavailable         = "log_export_unavailable"
	TooLarge            = "log_export_too_large"
)

const (
	ciphertextOverheadLimit = 64 << 10
	jsonDepthLimit          = 16
)

var ErrArtifact = errors.New("native_log_artifact_unavailable")
var ErrTooLarge = errors.New("native_log_artifact_too_large")
var segment = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)

type Target struct {
	PipelineVersion          string `json:"pipeline_version"`
	RunnerID                 string `json:"runner_id"`
	RunnerUUID               string `json:"runner_uuid"`
	RuntimeVersion           string `json:"runtime_version"`
	StateBackendType         string `json:"state_backend_type"`
	StateConfigurationDigest string `json:"state_configuration_digest"`
	ToolsDigest              string `json:"tools_digest"`
}

type Authority struct {
	OrganizationID  string `json:"organization_id"`
	EnvironmentID   string `json:"environment_id"`
	DeploymentID    string `json:"deployment_id"`
	CommandDigest   string `json:"command_digest"`
	Target          Target `json:"target"`
	Phase           string `json:"phase"`
	StepID          string `json:"step_id,omitempty"`
	ExecutionID     string `json:"execution_id"`
	CaptureRevision int64  `json:"capture_revision"`
	Subject         string `json:"subject,omitempty"`
	SourcePlanID    string `json:"source_plan_id,omitempty"`
	SourceSubject   string `json:"source_subject,omitempty"`
	Inherited       bool   `json:"inherited,omitempty"`
}

type Envelope struct {
	Version   int       `json:"version"`
	Authority Authority `json:"authority"`
	Status    string    `json:"status"`
	Logs      []byte    `json:"logs"`
}

type Descriptor struct {
	Version          int       `json:"version"`
	Authority        Authority `json:"authority"`
	Status           string    `json:"status"`
	CiphertextSHA256 string    `json:"ciphertext_sha256,omitempty"`
	CiphertextBytes  int       `json:"ciphertext_bytes,omitempty"`
	ObjectKey        string    `json:"object_key,omitempty"`
}

func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && strings.ToLower(value) == value
}

func (a Authority) Validate() error {
	for _, value := range []string{a.OrganizationID, a.EnvironmentID, a.DeploymentID, a.Target.RunnerID, a.Target.RunnerUUID, a.Target.StateBackendType, a.ExecutionID} {
		if !segment.MatchString(value) || value == "." || value == ".." {
			return ErrArtifact
		}
	}
	for _, value := range []string{a.Target.RuntimeVersion, a.Target.PipelineVersion} {
		if value == "" || len(value) > 128 || strings.ContainsAny(value, "\x00\r\n") {
			return ErrArtifact
		}
	}
	if a.CaptureRevision < 1 || !digest(a.CommandDigest) || !digest(a.Target.ToolsDigest) || !digest(a.Target.StateConfigurationDigest) {
		return ErrArtifact
	}
	if a.Phase != PlanPhase && a.Phase != ApplyPhase {
		return ErrArtifact
	}
	if a.StepID != "" || a.Subject != "" && !digest(a.Subject) {
		return ErrArtifact
	}
	if (a.SourcePlanID == "") != (a.SourceSubject == "") || a.SourcePlanID != "" && (!segment.MatchString(a.SourcePlanID) || !digest(a.SourceSubject)) {
		return ErrArtifact
	}
	if a.Inherited && (a.Phase != PlanPhase || a.SourcePlanID == "" || a.ExecutionID != a.SourcePlanID) {
		return ErrArtifact
	}
	body, err := json.Marshal(a)
	if err != nil || len(body) > MetadataLimit {
		return ErrArtifact
	}
	return nil
}

func (d Descriptor) Validate() error {
	if d.Version != 1 || d.Authority.Validate() != nil {
		return ErrArtifact
	}
	switch d.Status {
	case PendingTransfer:
		if !digest(d.CiphertextSHA256) || d.CiphertextBytes < 1 || d.CiphertextBytes > CiphertextLimit || d.ObjectKey != d.Authority.ObjectKey(d.CiphertextSHA256) {
			return ErrArtifact
		}
	case NotManaged, Unavailable, TooLarge:
		if d.CiphertextSHA256 != "" || d.CiphertextBytes != 0 || d.ObjectKey != "" {
			return ErrArtifact
		}
	default:
		return ErrArtifact
	}
	body, err := json.Marshal(d)
	if err != nil || len(body) > MetadataLimit {
		return ErrArtifact
	}
	return nil
}

func (a Authority) ObjectKey(hash string) string {
	return strings.Join([]string{a.EnvironmentID, "native", a.Target.RunnerUUID, a.DeploymentID, a.Phase, a.ExecutionID, hash}, "/")
}

// EventID binds a ready-event retry to the immutable capture tuple and bytes.
func (d Descriptor) EventID() string {
	return d.Authority.DeploymentID + ":native-log:" + d.Authority.Phase + ":" + d.Authority.ExecutionID + ":" + strconv.FormatInt(d.Authority.CaptureRevision, 10) + ":" + d.CiphertextSHA256
}

func (d Descriptor) VerifyCiphertext(content []byte) error {
	if d.Validate() != nil || d.Status != PendingTransfer || len(content) != d.CiphertextBytes || Hash(content) != d.CiphertextSHA256 {
		return ErrArtifact
	}
	return nil
}

// Encode encrypts once to all supplied recipients using standard age.
func Encode(envelope Envelope, recipients ...age.Recipient) ([]byte, error) {
	if len(envelope.Logs) > RawLimit {
		return nil, ErrTooLarge
	}
	if envelope.Version != 1 || envelope.Authority.Validate() != nil {
		return nil, ErrArtifact
	}
	switch envelope.Status {
	case PendingTransfer, NotManaged, Unavailable:
	case TooLarge:
		if len(envelope.Logs) != 0 {
			return nil, ErrArtifact
		}
	default:
		return nil, ErrArtifact
	}
	plain, err := json.Marshal(envelope)
	if err != nil {
		return nil, ErrArtifact
	}
	if len(plain) > EnvelopeLimit {
		return nil, ErrTooLarge
	}
	var output bytes.Buffer
	writer, err := age.Encrypt(&output, recipients...)
	if err != nil {
		return nil, err
	}
	if _, err = writer.Write(plain); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	if output.Len() > CiphertextLimit {
		return nil, ErrTooLarge
	}
	return output.Bytes(), nil
}

// Decode rejects partial plaintext, duplicate fields and trailing JSON before
// comparing all authority fields to the caller's retained reference.
func Decode(content []byte, identity age.Identity, expected Authority) (Envelope, error) {
	if len(content) > CiphertextLimit || expected.Validate() != nil {
		return Envelope{}, ErrArtifact
	}
	plain, err := Decrypt(content, identity, EnvelopeLimit)
	if err != nil {
		return Envelope{}, ErrArtifact
	}
	defer clear(plain)
	return DecodePlain(plain, expected)
}

func DecodePlain(plain []byte, expected Authority) (Envelope, error) {
	var envelope Envelope
	if expected.Validate() != nil {
		return Envelope{}, ErrArtifact
	}
	if StrictJSON(plain, &envelope, EnvelopeLimit) != nil || !exactEnvelope(plain) || envelope.Version != 1 || envelope.Authority != expected || len(envelope.Logs) > RawLimit {
		return Envelope{}, ErrArtifact
	}
	switch envelope.Status {
	case PendingTransfer, NotManaged, Unavailable:
	case TooLarge:
		if len(envelope.Logs) != 0 {
			return Envelope{}, ErrArtifact
		}
	default:
		return Envelope{}, ErrArtifact
	}
	return envelope, nil
}

func Decrypt(content []byte, identity age.Identity, limit int64) ([]byte, error) {
	reader, err := age.Decrypt(bytes.NewReader(content), identity)
	if err != nil {
		return nil, err
	}
	plain, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil || int64(len(plain)) > limit {
		return nil, ErrArtifact
	}
	return plain, nil
}

func StrictJSON(data []byte, value any, limit int) error {
	if len(data) > limit {
		return ErrArtifact
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if uniqueValue(decoder, 0) != nil || decoder.Decode(new(any)) != io.EOF {
		return ErrArtifact
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil || decoder.Decode(new(any)) != io.EOF {
		return ErrArtifact
	}
	return nil
}

func uniqueValue(decoder *json.Decoder, depth int) error {
	// Token does not enforce a nesting limit; bound the framing walk first.
	if depth > jsonDepthLimit {
		return ErrArtifact
	}
	token, err := decoder.Token()
	if err != nil {
		return ErrArtifact
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return ErrArtifact
	}
	seen := map[string]bool{}
	for decoder.More() {
		if delim == '{' {
			token, err = decoder.Token()
			name, ok := token.(string)
			if err != nil || !ok || seen[strings.ToLower(name)] {
				return ErrArtifact
			}
			seen[strings.ToLower(name)] = true
		}
		if uniqueValue(decoder, depth+1) != nil {
			return ErrArtifact
		}
	}
	_, err = decoder.Token()
	return err
}

func exactEnvelope(data []byte) bool {
	var body map[string]json.RawMessage
	if json.Unmarshal(data, &body) != nil {
		return false
	}
	logs, ok := body["logs"]
	if !ok || len(logs) == 0 || !bytes.Equal(logs, []byte("null")) && logs[0] != '"' {
		return false
	}
	delete(body, "logs")
	metadata, _ := json.Marshal(body)
	if !exactMembers(metadata, "version authority status") {
		return false
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	if !exactMembers(fields["authority"], "organization_id environment_id deployment_id command_digest target phase step_id execution_id capture_revision subject source_plan_id source_subject inherited") {
		return false
	}
	var authority map[string]json.RawMessage
	_ = json.Unmarshal(fields["authority"], &authority)
	return exactMembers(authority["target"], "pipeline_version runner_id runner_uuid runtime_version state_backend_type state_configuration_digest tools_digest")
}
