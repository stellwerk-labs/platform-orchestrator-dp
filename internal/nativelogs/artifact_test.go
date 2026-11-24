package nativelogs

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/stellwerk-labs/golib/hmessaging"
	"github.com/stretchr/testify/require"
)

func testAuthority() Authority {
	return Authority{
		OrganizationID:  "org",
		EnvironmentID:   "22222222-2222-4222-8222-222222222222",
		DeploymentID:    "33333333-3333-4333-8333-333333333333",
		CommandDigest:   strings.Repeat("a", 64),
		Target:          Target{RunnerID: "runner", RunnerUUID: "11111111-1111-4111-8111-111111111111", RuntimeVersion: "v2", PipelineVersion: "v2", ToolsDigest: strings.Repeat("b", 64), StateBackendType: "kubernetes", StateConfigurationDigest: strings.Repeat("c", 64)},
		Phase:           "plan",
		ExecutionID:     "33333333-3333-4333-8333-333333333333",
		CaptureRevision: 1,
	}
}

func TestNativeLogEnvelopeBoundedVerifiedEmptyAndFullContent(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	raw := make([]byte, RawLimit)
	_, err = rand.Read(raw)
	require.NoError(t, err)
	for _, logs := range [][]byte{nil, {}, raw} {
		a := testAuthority()
		ciphertext, err := Encode(Envelope{Version: 1, Authority: a, Status: PendingTransfer, Logs: logs}, identity.Recipient())
		require.NoError(t, err)
		envelope, err := Decode(ciphertext, identity, a)
		require.NoError(t, err)
		require.Equal(t, logs, envelope.Logs)
		different := a
		different.CaptureRevision++
		_, err = Decode(ciphertext, identity, different)
		require.Error(t, err)
		ciphertext[len(ciphertext)-1] ^= 1
		_, err = Decode(ciphertext, identity, a)
		require.Error(t, err)
	}
	_, err = Encode(Envelope{Version: 1, Authority: testAuthority(), Status: PendingTransfer, Logs: make([]byte, RawLimit+1)}, identity.Recipient())
	require.ErrorIs(t, err, ErrTooLarge)
}

func TestNativeLogExactMetadataAndInnerEnvelope(t *testing.T) {
	a := testAuthority()
	d := Descriptor{Version: 1, Authority: a, Status: PendingTransfer, CiphertextSHA256: strings.Repeat("d", 64), CiphertextBytes: 100}
	d.ObjectKey = a.ObjectKey(d.CiphertextSHA256)
	raw, err := json.Marshal(d)
	require.NoError(t, err)
	_, err = ParseDescriptor(raw)
	require.NoError(t, err)
	for _, changed := range []string{
		strings.Replace(string(raw), `"status":`, `"status ciphertext_sha256":"evil","status":`, 1),
		strings.Replace(string(raw), `"status":`, `"STATUS":`, 1),
		strings.Replace(string(raw), `"version":1`, `"version":null`, 1),
		strings.Replace(string(raw), `"version":1`, `"version":1,"version":1`, 1),
		string(raw) + ` {}`, strings.Replace(string(raw), `"target":{`, `"target":{"runner_id runner_uuid":"evil",`, 1),
	} {
		_, err := ParseDescriptor([]byte(changed))
		require.Error(t, err, changed)
	}
	plain, err := json.Marshal(Envelope{Version: 1, Authority: a, Status: PendingTransfer, Logs: nil})
	require.NoError(t, err)
	_, err = DecodePlain(plain, a)
	require.NoError(t, err, "explicit logs:null is an empty captured log")
	for _, changed := range []string{
		strings.Replace(string(plain), `"authority":`, `"AUTHORITY":`, 1),
		strings.Replace(string(plain), `"version":1`, `"version":null`, 1),
		strings.Replace(string(plain), `"status":`, `"status logs":"evil","status":`, 1),
		strings.Replace(string(plain), `,"logs":null`, "", 1),
		strings.Replace(string(plain), `"target":{`, `"target":{"RUNNER_ID":"runner",`, 1),
		string(plain) + ` null`,
	} {
		_, err := DecodePlain([]byte(changed), a)
		require.Error(t, err, changed)
	}
	route, err := hmessaging.NativeRunnerToken(a.Target.RunnerUUID)
	require.NoError(t, err)
	event := hmessaging.EventEnvelope{ProtocolVersion: hmessaging.ProtocolVersionV1, OrganizationID: a.OrganizationID, RunnerID: route, DeploymentID: a.DeploymentID, EventID: d.EventID(), Type: ReadyEventType, CreatedAt: time.Now().UTC(), Payload: raw}
	wire, err := json.Marshal(event)
	require.NoError(t, err)
	_, _, err = ParseReady(wire)
	require.NoError(t, err)
	event.EventID += ":foreign"
	wire, err = json.Marshal(event)
	require.NoError(t, err)
	_, _, err = ParseReady(wire)
	require.Error(t, err)
	_, err = ParseDescriptor(bytes.Repeat([]byte("x"), MetadataLimit+1))
	require.Error(t, err)
}

func TestStrictJSONBoundsNestingBeforeTypedDecode(t *testing.T) {
	// RawMessage accepts either shape, isolating the framing walk from typed
	// field/type refusal. These small inputs also support the old/new probe.
	for _, test := range []struct {
		name, open, close string
		depth             int
		accepted          bool
	}{
		{"array boundary", "[", "]", 16, true},
		{"array over boundary", "[", "]", 17, false},
		{"array excessive", "[", "]", 64, false},
		{"object boundary", `{"child":`, "}", 16, true},
		{"object over boundary", `{"child":`, "}", 17, false},
		{"object excessive", `{"child":`, "}", 64, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := []byte(strings.Repeat(test.open, test.depth) + "0" + strings.Repeat(test.close, test.depth))
			var raw json.RawMessage
			err := StrictJSON(data, &raw, len(data))
			if test.accepted {
				require.NoError(t, err)
				require.Equal(t, string(data), string(raw))
			} else {
				require.ErrorIs(t, err, ErrArtifact)
				require.Nil(t, raw, "framing refuses before typed decoding")
			}
		})
	}
}
