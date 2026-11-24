package model

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/require"
)

func testNativeTarget() *cp.NativeExecutionTargetReference {
	return &cp.NativeExecutionTargetReference{
		RunnerId:                 "native_pool",
		RunnerUuid:               uuid.MustParse("11111111-1111-4111-8111-111111111111"),
		RuntimeVersion:           cp.NativeTargetRuntimeV2,
		PipelineVersion:          cp.NativeTargetPipelineV2,
		ToolsDigest:              strings.Repeat("a", 64),
		StateBackendType:         cp.StateStorageType("s3"),
		StateConfigurationDigest: strings.Repeat("b", 64),
	}
}

func TestObservationFingerprintRetainsLegacyBytesAndBindsEveryNativeField(t *testing.T) {
	fingerprint := func(target *cp.NativeExecutionTargetReference) string {
		return ObservationBaselineFingerprint(EncodedDeploymentManifest(`{"workloads":{}}`), RawTofu("terraform {}"), EncodedDeploymentGraph(`{}`), nil, target)
	}
	legacy := fingerprint(nil)
	require.Equal(t, "sha256:0ab7ee66f840e33aa129d09e985e5a036e65b67974f014155afc32cab8d40e48", legacy)
	original := testNativeTarget()
	native := fingerprint(original)
	require.NotEqual(t, legacy, native)
	for _, change := range []func(*cp.NativeExecutionTargetReference){
		func(v *cp.NativeExecutionTargetReference) { v.RunnerId = "another" }, func(v *cp.NativeExecutionTargetReference) { v.RunnerUuid = uuid.New() },
		func(v *cp.NativeExecutionTargetReference) { v.RuntimeVersion = "other" }, func(v *cp.NativeExecutionTargetReference) { v.PipelineVersion = "other" },
		func(v *cp.NativeExecutionTargetReference) { v.ToolsDigest = "different" }, func(v *cp.NativeExecutionTargetReference) { v.StateBackendType = "gcs" }, func(v *cp.NativeExecutionTargetReference) { v.StateConfigurationDigest = "different" },
	} {
		changed := *original
		change(&changed)
		require.NotEqual(t, native, fingerprint(&changed))
	}
}

func TestQueueCallerIdentityOmitsResolvedNativeTargetBeforeLegacyEarlyReturn(t *testing.T) {
	for _, legacy := range []string{`{ "manifest": {} }`, `{"pipeline":{"name":"old"},"manifest":{}}`, `{"pipeline":{"request_identity_version":1,"requested_selection":null},"manifest":{}}`} {
		var request map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(legacy), &request))
		target, err := json.Marshal(testNativeTarget())
		require.NoError(t, err)
		request["native_target"] = target
		native, err := json.Marshal(request)
		require.NoError(t, err)
		require.JSONEq(t, string(queueRequestedIntent(json.RawMessage(legacy))), string(queueRequestedIntent(native)))
	}
	for _, native := range []string{
		`{"z":1,"a":2,"native_target":{}}`,
		`{"native_target":{},"z":1,"a":2}`,
		`{"z":1,"native_target":{},"a":2}`,
	} {
		legacy := NewDeploymentQueueItem{ProjectID: "p", EnvID: "e", Mode: DeploymentModeDeploy, Intent: json.RawMessage(`{"z":1,"a":2}`)}
		resolved := legacy
		resolved.Intent = json.RawMessage(native)
		require.Equal(t, legacy.IntentSHA256(), resolved.IntentSHA256())
	}
	legacy := json.RawMessage(`{ "manifest": {} }`)
	require.Equal(t, legacy, queueRequestedIntent(legacy), "unversioned legacy bytes are untouched")
}
